package fulltext

import (
	"golang.org/x/sync/errgroup"
	"strings"
	"sync"
)

func (fulltext *Fulltext) Suggest(index, query string) ([]string, error) {
	var mutex sync.Mutex
	var eg errgroup.Group
	var size int
	ts := make([]string, 0, 15)

	tokens := fulltext.tokenizer.Seg(query)

	l := len(tokens)
	if l < 4 {
		size = 4
	} else if l > 3 && l < 6 {
		size = 3
	} else {
		return nil, nil
	}

	for _, token := range tokens {
		if _, exist := fulltext.stopWords[token]; exist {
			continue
		}

		token := token
		eg.Go(func() error {

			tKs, err := fulltext.t(index, token, size)
			if err != nil {
				return err
			}
			mutex.Lock()
			defer mutex.Unlock()
			for _, t := range tKs {
				i := strings.LastIndex(t, ":")
				ts = append(ts, t[i+1:])
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return ts, nil
}
