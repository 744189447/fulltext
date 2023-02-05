package fulltext

import (
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"path"
)

type Fulltext struct {
	dbPath    string
	db        *leveldb.DB
	tokenizer Tokenizer
	stopWords map[string]struct{}
	k1        float32
	b         float32
	retSize   int
}

func New(filePath string, tokenizer Tokenizer) (*Fulltext, error) {
	if tokenizer == nil {
		return nil, errors.New("fulltext/new: tokenizer is nil")
	}
	stopWords := make(map[string]struct{})
	dbPath := path.Join(filePath, "db")
	o := &opt.Options{
		Filter: filter.NewBloomFilter(10),
	}
	db, err := leveldb.OpenFile(dbPath, o)
	if err != nil {
		return nil, err
	}

	lines, err := readLines(path.Join(filePath, "stop_word.txt"))
	if err != nil {
		return nil, err
	}

	for _, w := range lines {
		stopWords[w] = struct{}{}
	}
	stopWords[" "] = struct{}{}
	stopWords["\n"] = struct{}{}

	fulltext := &Fulltext{
		dbPath:    dbPath,
		db:        db,
		tokenizer: tokenizer,
		stopWords: stopWords,
		k1:        1.4,
		b:         0.75,
		retSize:   10,
	}
	return fulltext, nil
}

func (fulltext *Fulltext) Free() error {
	return fulltext.db.Close()
}
