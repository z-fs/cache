package cachelevel

import (
	fsobejctlevel "z-fs/fsObejctlevel"
)

/*
*
ZID,整个fs的路由
FileCache缓存在文件系统中的路由
*/
var timeCnt = 0

type CacheObj struct {
	merkleRoot string
	len        int
	fileCache  fsobejctlevel.Node
	deadMark   int
}

// 4
type Bucket struct {
	Caches []CacheObj
}

func (bk *Bucket) check(merkleRoot []byte, kvStore *CacheStore) (bool, fsobejctlevel.Node) {
	bf := newBloomFilter(len(bk.Caches), 0.1)
	for _, value := range bk.Caches {
		bf.Insert([]byte(value.merkleRoot))
	}

	if bf.Query(merkleRoot) {
		for i, val := range bk.Caches {
			if val.merkleRoot == string(merkleRoot) {
				timeCnt++
				bk.Caches[i].deadMark = timeCnt
				return true, val.fileCache
			}
		}
		return false, nil
	}
	return false, nil
}

func (bk *Bucket) update(val fsobejctlevel.Dir, merkleRoot string, kvStore *CacheStore) {
	cur := 0
	for i := 1; i < len(bk.Caches); i++ {
		if bk.Caches[i].deadMark < bk.Caches[cur].deadMark {
			cur = i
		}
		if bk.Caches[i].merkleRoot == merkleRoot {
			cur = i
			break
		}
	}
	timeCnt++
	if bk.Caches[cur].merkleRoot != merkleRoot {
		len := val.Size()
		kvStore.Delete([]byte(bk.Caches[cur].merkleRoot))
		bk.Caches[cur] = CacheObj{
			merkleRoot: merkleRoot,
			deadMark:   timeCnt,
			len:        int(len),
		}
		bk.Caches[cur].fileCache = val
		kvStore.Put([]byte(merkleRoot), []byte(merkleRoot))
	} else {
		bk.Caches[cur].deadMark = timeCnt
	}
}

func newBucket() Bucket {
	return Bucket{
		Caches: make([]CacheObj, 4),
	}
}
