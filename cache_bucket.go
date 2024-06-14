package cachelevel

import (
	"strings"
	fsobejctlevel "z-fs/fsObejctlevel"
)

const N = 1 << 8
const P = 4
const CacheLimit = 512 * 1024 * 1024 // 512 MB

type Buckets struct {
	table   []Bucket
	kvStore *CacheStore
}

func (bk *Buckets) Insert(fsObj fsobejctlevel.Node, merkleRoot []byte) {
	index := merkleRoot[0]
	flag, _ := bk.table[index].check(merkleRoot, bk.kvStore)
	if flag {
		return
	}
	bk.table[index].update(fsObj.(*fsobejctlevel.ZLDDir), string(merkleRoot), bk.kvStore)
}

func (bk *Buckets) Get(zid string) []byte {

	filepath := strings.Split(zid, "/")
	merkleRoot := []byte(filepath[0])
	index := merkleRoot[0]
	flag, ans := bk.table[index].check([]byte(filepath[0]), bk.kvStore)
	if flag {
		fsObj := ans
		return getFileByDir(fsObj.(fsobejctlevel.Dir), filepath[1:])
	} else {
		return nil
	}
}

func getFileByDir(dir fsobejctlevel.Dir, filepath []string) []byte {
	for _, path := range filepath {
		it := dir.It()
		for it.Next() {
			node := it.Node()
			if node.Name() != path {
				continue
			}
			if node.Type() == fsobejctlevel.FILE {
				return node.(fsobejctlevel.File).Bytes()
			} else {
				dir = node.(fsobejctlevel.Dir)
			}
		}
	}
	return nil
}

func New() *Buckets {
	bts := &Buckets{
		table:   reStore(),
		kvStore: newLevelDB("./cachelevel/cache.db"),
	}
	defer bts.kvStore.Close()
	// iter := bts.kvStore.NewIterator(nil, nil)
	// for iter.Next() {
	// 	key := iter.Key()
	// 	value := iter.Value()
	// 	index := key[0]
	// 	for i := 0; i < 4; i++ {
	// 		if bts.table[index].Caches[i].merkleRoot == "" {
	// 			bts.table[index].Caches[i] = CacheObj{
	// 				len:        len(value),
	// 				deadMark:   0,
	// 				merkleRoot: string(key),
	// 			}
	// 			break
	// 		}
	// 	}
	// }
	// iter.Release()
	return bts
}

func reStore() []Bucket {
	table := make([]Bucket, N)
	for i := 0; i < N; i++ {
		table[i] = newBucket()
	}
	return table
}
