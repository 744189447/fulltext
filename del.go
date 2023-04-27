package fulltext

import (
	"context"
	"errors"
	"github.com/nextzhou/workpool"
	"os"
	"sync"
)

type docT struct {
	idK []byte
	t   map[string]map[string]struct{}
	len uint32
}

func (fulltext *Fulltext) DelDB() error {
	return os.RemoveAll(fulltext.dbPath)
}

func (fulltext *Fulltext) DelIndex(index string) error {
	return fulltext.removeIndex(index)
}

func (fulltext *Fulltext) DelDocs(index string, docsID ...string) error {
	l := len(docsID)
	if l == 0 {
		return nil
	} else if l > 1000 {
		return errors.New("fulltext/del: too much docs")
	}

	docsT := make([]docT, 0, l)

	if l == 1 {
		ok, doc, err := fulltext.docT(index, docsID[0])
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		docsT = append(docsT, doc)
	} else { // batch
		var limit uint = 5
		if l < 5 {
			limit = uint(l)
		}

		var mutex sync.Mutex
		wp := workpool.New(context.TODO(), workpool.Options.ParallelLimit(limit))
		for _, id := range docsID {
			id := id
			wp.Go(func(ctx context.Context) error {
				ok, doc, err := fulltext.docT(index, id)
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}

				mutex.Lock()
				defer mutex.Unlock()
				docsT = append(docsT, doc)
				return nil
			})
		}

		if err := wp.Wait(); err != nil {
			return err
		}
	}

	if len(docsT) == 0 {
		return nil
	}

	tf := make(map[string]map[string]uint32)
	idf := make(map[string]uint32)
	idsK := make([][]byte, 0, len(docsT))

	ts, err := fulltext.ts(index)
	if err != nil {
		return err
	}

	ds, err := fulltext.ds(index)
	if err != nil {
		return err
	}

	for _, doc := range docsT {

		ts -= uint64(doc.len)
		ds--

		for token, idT := range doc.t {
			if _, exist := tf[token]; !exist {
				tfVal, err := fulltext.tf(index, token)
				if err != nil {
					return err
				}
				tf[token] = tfVal
			}

			for id := range idT {
				delete(tf[token], id)
				if _, exist := idf[token]; !exist {
					idfVal, err := fulltext.idf(index, token)
					if err != nil {
						return err
					}
					idf[token] = idfVal - 1
				} else {
					idf[token]--
				}
			}
		}
		idsK = append(idsK, doc.idK)
	}

	if err = fulltext.removeMeta(index, tf, idf, idsK, ts, ds); err != nil {
		return err
	}

	return nil
}
