package fulltext

import (
	"golang.org/x/sync/errgroup"
	"math"
	"sort"
	"sync"
	"time"
)

type Hits struct {
	Total int
	Took  int
	Docs  []Doc
}

type Doc struct {
	ID    string
	Score float32
}

type Query struct {
	index   string
	match   string
	must    []string
	should  []string
	mustNot []string
	from    int
	size    int
}

func (query *Query) Index(str string) *Query {
	query.index = str
	return query
}

func (query *Query) Match(str string) *Query {
	query.match = str
	return query
}

func (query *Query) Must(str ...string) *Query {
	query.must = str
	return query
}

func (query *Query) Should(str ...string) *Query {
	query.should = str
	return query
}

func (query *Query) MustNot(str ...string) *Query {
	query.mustNot = str
	return query
}

func (query *Query) Limit(from, size int) *Query {
	query.from = from
	query.size = size
	return query
}

type tokenTFIDF struct {
	tokenTF  map[string]uint32
	tokenIDF uint32
}

func (fulltext *Fulltext) Search(query *Query) (*Hits, error) {
	start := time.Now()
	hits := new(Hits)
	var (
		err               error
		scores            map[string]float32
		tokensTF          map[string]map[string]uint32
		total             int
		match             []Doc
		mustTF, mustNotTF []map[string]uint32
		tokensTFIDF       []tokenTFIDF
	)

	if query == nil {
		goto final
	}

	if query.index == "" {
		goto final
	}

	tokensTFIDF = make([]tokenTFIDF, 0, 7)

	if query.match != "" {
		var mutex sync.Mutex
		var eg errgroup.Group
		tokensTF = make(map[string]map[string]uint32)

		tokens := fulltext.tokenizer.Seg(query.match)

		for _, token := range tokens {
			token := token

			if _, exist := fulltext.stopWords[token]; exist {
				continue
			}

			eg.Go(func() error {
				tf, err := fulltext.tf(query.index, token)
				if err != nil {
					return err
				}
				if tf == nil {
					return nil
				}

				idf, err := fulltext.idf(query.index, token)
				if err != nil {
					return err
				}

				mutex.Lock()
				defer mutex.Unlock()

				tokensTF[token] = tf

				tokensTFIDF = append(tokensTFIDF, tokenTFIDF{tokenTF: tf, tokenIDF: idf})

				return nil
			})
		}

		if err = eg.Wait(); err != nil {
			return nil, err
		}
	}

	if len(query.should) != 0 {
		for _, shouldStr := range query.should {
			if _, exist := tokensTF[shouldStr]; !exist {
				tf, err := fulltext.tf(query.index, shouldStr)
				if err != nil {
					return nil, err
				}
				if tf != nil {
					idf, err := fulltext.idf(query.index, shouldStr)
					if err != nil {
						return nil, err
					}
					tokensTFIDF = append(tokensTFIDF, tokenTFIDF{tokenTF: tf, tokenIDF: idf})
				}
			}
		}
	}

	if len(query.must) != 0 {
		for _, mustStr := range query.must {
			if tokenTF, exist := tokensTF[mustStr]; exist {

				mustTF = append(mustTF, tokenTF)
			} else {
				tf, err := fulltext.tf(query.index, mustStr)
				if err != nil {
					return nil, err
				}
				if tf != nil {
					idf, err := fulltext.idf(query.index, mustStr)
					if err != nil {
						return nil, err
					}
					tokensTFIDF = append(tokensTFIDF, tokenTFIDF{tokenTF: tf, tokenIDF: idf})

					mustTF = append(mustTF, tf)
				}
			}
		}
	}

	if len(tokensTFIDF) == 0 {
		goto final
	}

	if len(query.mustNot) != 0 {
		for _, mustNotStr := range query.mustNot {
			if tokenTF, exist := tokensTF[mustNotStr]; exist {

				mustNotTF = append(mustNotTF, tokenTF)
			} else {
				tf, err := fulltext.tf(query.index, mustNotStr)
				if err != nil {
					return nil, err
				}
				if tf != nil {
					mustNotTF = append(mustNotTF, tf)
				}
			}
		}
	}

	for _, tfidf := range tokensTFIDF {
		for id := range tfidf.tokenTF {
			for _, tf := range mustTF {
				if _, exist := tf[id]; !exist {
					delete(tfidf.tokenTF, id)
				}
			}
			for _, tf := range mustNotTF {
				if _, exist := tf[id]; exist {
					delete(tfidf.tokenTF, id)
				}
			}
		}
	}

	scores, err = fulltext.score(query.index, tokensTFIDF)
	if err != nil {
		return nil, err
	}

	if len(scores) == 0 {
		goto final
	}

	match = make([]Doc, 0, len(scores))
	for id, score := range scores {
		match = append(match, Doc{ID: id, Score: score})
	}
	total = len(match)
	sort.Slice(match, func(i, j int) bool {
		if match[i].Score != match[j].Score {
			return match[i].Score > match[j].Score
		}
		return match[i].ID < match[j].ID
	})

	if query.from >= total {
		goto final
	}

	if query.size == 0 {
		query.size = fulltext.retSize
	}

	for i := query.from; i < total; i++ {
		if i < total && i < query.from+query.size {
			hits.Docs = append(hits.Docs, match[i])
		} else {
			break
		}
	}
	hits.Total = total

final:
	hits.Took = int(time.Now().Sub(start).Milliseconds())

	return hits, nil
}

func (fulltext *Fulltext) score(i string, tokensTFIDF []tokenTFIDF) (map[string]float32, error) {
	ts, err := fulltext.ts(i)
	if err != nil {
		return nil, err
	}

	ds, err := fulltext.ds(i)
	if err != nil {
		return nil, err
	}

	var mean float32
	if ds != 0 {
		mean = float32(float64(ts) / float64(ds))
	}

	scores := make(map[string]float32)

	for _, tfidf := range tokensTFIDF {
		if len(tfidf.tokenTF) != 0 {

			idf := float32(math.Log(float64(1 + (float32(ds)-float32(tfidf.tokenIDF)+0.5)/(float32(tfidf.tokenIDF)+0.5))))

			for id, tfVal := range tfidf.tokenTF {

				tf := (float32(tfVal) * (fulltext.k1 + 1)) / (float32(tfVal) + fulltext.k1*(1-fulltext.b+fulltext.b*(float32(ds)/mean)))

				if _, exist := scores[id]; !exist {
					scores[id] = tf * idf
				} else {
					scores[id] += tf * idf
				}
			}
		}
	}

	return scores, nil
}
