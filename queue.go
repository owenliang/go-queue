package go_queue

import (
	"sync"
	"github.com/jmhodges/levigo"
	"encoding/binary"
)

type GoQueue struct {
	db *levigo.DB
	mutex sync.Mutex
	cond *sync.Cond
	readPosition uint64
	writePosition uint64
	woptions *levigo.WriteOptions
	roptions *levigo.ReadOptions
}

func CreateQueue(dbname string) (*GoQueue, error) {
	options := levigo.NewOptions()
	options.SetCreateIfMissing(true)

	woptions := levigo.NewWriteOptions()
	roptions := levigo.NewReadOptions()

	db, err := levigo.Open(dbname, options)
	if err != nil {
		return nil, err
	}

	var readPosition uint64 = 0
	var writePosition uint64 = 0

	readBufs, err := db.Get(roptions, []byte("_readPosition"))
	if err != nil {
		return nil, err
	} else if readBufs != nil {
		readPosition = binary.BigEndian.Uint64(readBufs)
	}
	writeBufs, err := db.Get(roptions, []byte("_writePosition"))
	if err != nil {
		return nil, err
	} else if writeBufs != nil {
		writePosition = binary.BigEndian.Uint64(writeBufs)
	}

	q := GoQueue{
		db: db,
		readPosition: readPosition,
		writePosition: writePosition,
		woptions: woptions,
		roptions: roptions,
	}
	q.cond = sync.NewCond(&q.mutex)
	return &q, nil
}

func (q *GoQueue) Push(data []byte) (bool, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	pos := make([]byte, 8)
	binary.BigEndian.PutUint64(pos, q.writePosition)
	if err := q.db.Put(q.woptions, pos, data); err != nil {
		return false, err
	}

	binary.BigEndian.PutUint64(pos, q.writePosition + 1)
	if err := q.db.Put(q.woptions, []byte("_writePosition"), pos); err != nil {
		return false, err
	}
	q.writePosition = q.writePosition + 1
	q.cond.Signal()
	return true, nil
}

func (q *GoQueue) Pop() ([]byte, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	for ; q.readPosition >= q.writePosition; {
		q.cond.Wait()
	}
	pos := make([]byte, 8)
	binary.BigEndian.PutUint64(pos, q.readPosition)
	value, err := q.db.Get(q.roptions, pos)
	if err != nil {	// error
		return nil, err
	}
	if value != nil { // exists
		if err = q.db.Delete(q.woptions, pos); err != nil {
			return nil, err
		}
	}
	binary.BigEndian.PutUint64(pos, q.readPosition + 1)
	err = q.db.Put(q.woptions, []byte("_readPosition"), pos)
	if err != nil {
		return nil, err
	}
	q.readPosition = q.readPosition + 1
	return value, nil
}

func (q *GoQueue)DestroyQueue() {
	q.db.Close()
}

func (q *GoQueue)Stats() string {
	stats := q.db.PropertyValue("leveldb.stats")
	return stats
}