package cachelevel

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type CacheStore struct {
	db *leveldb.DB
}

func newLevelDB(path string) *CacheStore {
	db, _ := leveldb.OpenFile(path, nil)
	return &CacheStore{
		db: db,
	}
}

func (db *CacheStore) Close() error {
	return db.db.Close()
}

func (db *CacheStore) Has(key []byte) (bool, error) {
	return db.db.Has(key, nil)
}

func (db *CacheStore) Put(key, value []byte) error {
	//h := hash.Sha3Sum256(value)
	err := db.db.Put(key, value, nil)
	return err
}

func (db *CacheStore) Get(key []byte) ([]byte, error) {
	return db.db.Get(key, nil)
}

func (db *CacheStore) Delete(key []byte) error {
	return db.db.Delete(key, nil)
}

func (db *CacheStore) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	return db.db.NewIterator(slice, ro)
}
