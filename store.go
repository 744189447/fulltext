package fulltext

import (
	"errors"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func (fulltext *Fulltext) tf(i string, token string) (map[string]uint32, error) {
	key := []byte(fmt.Sprintf("%s:%s:%s:%s", indexKey, i, tfKey, token))
	val, err := fulltext.db.Get(key, nil)
	if err != nil && !errors.Is(err, leveldb.ErrNotFound) {
		return nil, err
	}
	if len(val) == 0 {
		return nil, nil
	}

	match := make(map[string]uint32)
	err = byteToAny(val, &match)
	if err != nil {
		return nil, err
	}

	return match, nil
}

func (fulltext *Fulltext) idf(i string, token string) (uint32, error) {
	key := []byte(fmt.Sprintf("%s:%s:%s:%s", indexKey, i, idfKey, token))
	val, err := fulltext.db.Get(key, nil)
	if err != nil && !errors.Is(err, leveldb.ErrNotFound) {
		return 0, err
	}
	if len(val) == 0 {
		return 0, nil
	}
	idfVal := byteToUint32(val)

	return idfVal, nil
}

func (fulltext *Fulltext) ts(i string) (uint64, error) {
	key := []byte(fmt.Sprintf("%s:%s:%s", indexKey, i, tsKey))
	val, err := fulltext.db.Get(key, nil)
	if err != nil && !errors.Is(err, leveldb.ErrNotFound) {
		return 0, err
	}
	if len(val) == 0 {
		return 0, nil
	}
	termSize := byteToUint64(val)

	return termSize, nil
}

func (fulltext *Fulltext) ds(i string) (uint32, error) {
	key := []byte(fmt.Sprintf("%s:%s:%s", indexKey, i, dsKey))
	val, err := fulltext.db.Get(key, nil)
	if err != nil && !errors.Is(err, leveldb.ErrNotFound) {
		return 0, err
	}
	if len(val) == 0 {
		return 0, nil
	}
	docSize := byteToUint32(val)

	return docSize, nil
}

func (fulltext *Fulltext) docT(i, id string) (ret bool, doc docT, err error) {
	content := make(map[string]map[string]struct{})
	idKey := []byte(fmt.Sprintf("%s:%s:%s:%s", indexKey, i, docKey, id))
	tv, err := fulltext.db.Get(idKey, nil)
	if err != nil && !errors.Is(err, leveldb.ErrNotFound) {
		return
	}

	if len(tv) == 0 {
		return false, docT{}, nil
	}

	ts := idTS{}
	err = byteToAny(tv, &ts)
	if err != nil {
		return
	}

	for _, token := range ts.T {
		content[token] = make(map[string]struct{})
		content[token][id] = struct{}{}
	}
	ret = true
	doc = docT{idKey, content, ts.S}

	return
}

func (fulltext *Fulltext) t(i string, token string, size int) ([]string, error) {
	key := []byte(fmt.Sprintf("%s:%s:%s:%s", indexKey, i, tfKey, token))
	var tsK []string
	var count int
	iter := fulltext.db.NewIterator(util.BytesPrefix(key), nil)
	for iter.Next() {
		if count == size {
			break
		} else {
			tsK = append(tsK, string(iter.Key()))
		}
		count++
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return nil, err
	}

	return tsK, nil
}

func (fulltext *Fulltext) addMeta(i string, tf map[string]map[string]uint32, idf map[string]uint32, idt map[string]idTS, ts uint64, ds uint32) error {
	batch := new(leveldb.Batch)
	for k, v := range tf {
		val, err := anyToByte(v)
		if err != nil {
			return err
		}

		key := []byte(fmt.Sprintf("%s:%s:%s:%s", indexKey, i, tfKey, k))
		batch.Put(key, val)
	}

	for k, v := range idf {
		val := uint32ToByte(v)
		key := []byte(fmt.Sprintf("%s:%s:%s:%s", indexKey, i, idfKey, k))
		batch.Put(key, val)
	}

	for k, v := range idt {
		val, err := anyToByte(v)
		if err != nil {
			return err
		}

		key := []byte(fmt.Sprintf("%s:%s:%s:%s", indexKey, i, docKey, k))
		batch.Put(key, val)
	}

	tsK := []byte(fmt.Sprintf("%s:%s:%s", indexKey, i, tsKey))
	tsV := uint64ToByte(ts)
	batch.Put(tsK, tsV)

	dsK := []byte(fmt.Sprintf("%s:%s:%s", indexKey, i, dsKey))
	dsV := uint32ToByte(ds)
	batch.Put(dsK, dsV)

	err := fulltext.db.Write(batch, nil)
	if err != nil {
		return err
	}

	return nil
}

func (fulltext *Fulltext) removeMeta(i string, tf map[string]map[string]uint32, idf map[string]uint32, idsK [][]byte, ts uint64, ds uint32) error {
	batch := new(leveldb.Batch)
	for k, v := range tf {
		val, err := anyToByte(v)
		if err != nil {
			return err
		}
		key := []byte(fmt.Sprintf("%s:%s:%s:%s", indexKey, i, tfKey, k))
		if len(v) == 0 {
			batch.Delete(key)
		} else {
			batch.Put(key, val)
		}
	}

	for k, v := range idf {
		key := []byte(fmt.Sprintf("%s:%s:%s:%s", indexKey, i, idfKey, k))
		if v == 0 {
			batch.Delete(key)
		} else {
			val := uint32ToByte(v)
			batch.Put(key, val)
		}
	}

	for _, idK := range idsK {
		batch.Delete(idK)
	}

	tsK := []byte(fmt.Sprintf("%s:%s:%s", indexKey, i, tsKey))
	if ts == 0 {
		batch.Delete(tsK)
	} else {
		tsV := uint64ToByte(ts)
		batch.Put(tsK, tsV)
	}

	dsK := []byte(fmt.Sprintf("%s:%s:%s", indexKey, i, dsKey))
	if ds == 0 {
		batch.Delete(dsK)
	} else {
		dsV := uint32ToByte(ds)
		batch.Put(dsK, dsV)
	}

	err := fulltext.db.Write(batch, nil)
	if err != nil {
		return err
	}

	return nil
}

func (fulltext *Fulltext) removeIndex(i string) error {
	key := []byte(fmt.Sprintf("%s:%s:", indexKey, i))

	batch := new(leveldb.Batch)
	var count int
	iter := fulltext.db.NewIterator(util.BytesPrefix(key), nil)
	for iter.Next() {
		if count == 20000 {
			err := fulltext.db.Write(batch, nil)
			if err != nil {
				return err
			}
			batch.Reset()
			count = 0
		}
		batch.Delete(iter.Key())
		count++
	}

	iter.Release()
	err := iter.Error()
	if err != nil {
		return err
	}

	if count > 0 {
		err = fulltext.db.Write(batch, nil)
		if err != nil {
			return err
		}
	}

	return nil
}
