package fulltext

import (
	"context"
	"errors"
	"github.com/nextzhou/workpool"
	"sync"
)

type docMeta struct {
	id  string
	tf  map[string]map[string]uint32
	len int
}

type idTS struct {
	T []string
	S uint32
}

func (fulltext *Fulltext) AddDocs(index string, docs map[string]string) error {
	l := len(docs)
	if l == 0 {
		return nil
	} else if l > 1000 {
		return errors.New("fulltext/add: too much docs")
	}

	tf := make(map[string]map[string]uint32)
	idf := make(map[string]uint32)
	docsMeta := make([]docMeta, 0, l)

	if l == 1 {
		for id, doc := range docs {
			docsMeta = append(docsMeta, fulltext.analyse(id, doc))
		}
	} else { // batch
		var limit uint = 5
		if l < 5 {
			limit = uint(l)
		}

		var mutex sync.Mutex
		wp := workpool.New(context.TODO(), workpool.Options.ParallelLimit(limit))
		for id, doc := range docs {
			id := id
			doc := doc
			wp.Go(func(ctx context.Context) error {
				mutex.Lock()
				defer mutex.Unlock()
				docsMeta = append(docsMeta, fulltext.analyse(id, doc))
				return nil
			})
		}
		if err := wp.Wait(); err != nil {
			return err
		}
	}

	ts, err := fulltext.ts(index)
	if err != nil {
		return err
	}

	ds, err := fulltext.ds(index)
	if err != nil {
		return err
	}

	idt := make(map[string]idTS)
	for _, meta := range docsMeta {

		ts += uint64(meta.len)
		ds++

		t := make([]string, 0, len(meta.tf))
		for token, idTF := range meta.tf {
			t = append(t, token)

			if _, exist := tf[token]; !exist {
				tfVal, err := fulltext.tf(index, token)
				if err != nil {
					return err
				}

				if tfVal != nil {
					tf[token] = tfVal
				} else {
					tf[token] = make(map[string]uint32)
				}
			}

			for id, tfVal := range idTF {
				if _, exist := idf[token]; !exist {
					idfVal, err := fulltext.idf(index, token)
					if err != nil {
						return err
					}
					idf[token] = idfVal + 1
				} else {
					idf[token]++
				}

				tf[token][id] = tfVal
			}
		}

		idt[meta.id] = idTS{t, uint32(meta.len)}
	}

	if err = fulltext.addMeta(index, tf, idf, idt, ts, ds); err != nil {
		return err
	}

	return nil
}

func (fulltext *Fulltext) analyse(id string, doc string) docMeta {
	content := make(map[string]map[string]uint32)
	tokens := fulltext.tokenizer.Seg(doc)

	tf, ts := fulltext.termFreq(tokens)

	for t, f := range tf {
		content[t] = make(map[string]uint32)
		content[t][id] = f
	}

	return docMeta{id, content, ts}
}

func (fulltext *Fulltext) termFreq(tokens []string) (map[string]uint32, int) {
	tf := make(map[string]uint32)
	var ts int
	for _, token := range tokens {
		if _, exist := fulltext.stopWords[token]; exist {
			continue
		}
		tf[token]++
		ts++
	}
	return tf, ts
}
