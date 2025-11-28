package lsmkv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/charithesh16/lsm-kv/memtable"
	"github.com/charithesh16/lsm-kv/sstable"
	"github.com/charithesh16/lsm-kv/wal"
)

type KVStore struct {
	memtable          *memtable.Memtable
	immutableMemtable []*memtable.Memtable
	sstables          []*sstable.SSTable
	wal               *wal.WAL
	mu                sync.Mutex
	sequenceNumber    int64
	dir               string
}

var ErrKeyNotFound = errors.New("key not found")

func NewKVStore(dir string) (*KVStore, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	// Ensure sstable directory exists
	sstableDir := filepath.Join(dir, "sstable")
	if err := os.MkdirAll(sstableDir, 0755); err != nil {
		return nil, err
	}

	// Load SSTables and get the max Sequence Number
	sstables, err := filepath.Glob(filepath.Join(sstableDir, "*.sst"))
	if err != nil {
		return nil, err
	}

	var loadedSSTables []*sstable.SSTable
	var maxSeqNum int64 = -1

	//TODO: load in the order of sequence number
	for _, sstablePath := range sstables {
		// Parse sequence number from filename "data-0000.sst"
		baseName := filepath.Base(sstablePath)
		var seqNum int64
		_, err := fmt.Sscanf(baseName, "data-%04d.sst", &seqNum)
		if err == nil {
			if seqNum > maxSeqNum {
				maxSeqNum = seqNum
			}
		}

		ss, err := sstable.NewSSTable(sstablePath)
		if err != nil {
			return nil, err
		}
		loadedSSTables = append(loadedSSTables, ss)
	}

	// Next sequence number should be greater than any existing SSTable
	sequenceNumber := maxSeqNum + 1

	//TODO: add logic to handle case where WAL present but no sstable, need to write memtable to sstable and delete WAL
	//TODO: add code to handle corrupted SStable write to new SStable and delete corrupted SStable

	// Recover from WAL, WAL without sstable is starting point
	walDir := filepath.Join(dir, "wal")
	walPath := filepath.Join(walDir, fmt.Sprintf("wal-%04d.log", sequenceNumber))

	mem := memtable.NewMemtable()
	// Only try to recover if the file actually exists
	if _, err := os.Stat(walPath); err == nil {
		if err := RecoverMemtableFromWAL(walPath, mem); err != nil {
			return nil, err
		}
	}

	// Create new WAL instance
	wal, err := wal.NewWAL(dir, sequenceNumber)
	if err != nil {
		return nil, err
	}

	return &KVStore{
		memtable:          mem,
		wal:               wal,
		immutableMemtable: nil,
		sequenceNumber:    sequenceNumber,
		sstables:          loadedSSTables,
		dir:               dir,
	}, nil
}

func (kv *KVStore) Put(key, value []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// write to wal first
	err := kv.wal.Write(key, value)
	if err != nil {
		return err
	}

	// write to memtable
	kv.memtable.Put(key, value)
	if kv.memtable.IsFull() {
		kv.rotateAndFlush()
	}
	return nil
}

func (kv *KVStore) Get(key []byte) ([]byte, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if v, ok := kv.memtable.Get(key); ok {
		if v == nil {
			return nil, ErrKeyNotFound
		} // Tombstone
		return v, nil
	}

	// Check Immutable Memtables (Newest to Oldest)
	for i := len(kv.immutableMemtable) - 1; i >= 0; i-- {
		if v, ok := kv.immutableMemtable[i].Get(key); ok {
			if v == nil {
				return nil, ErrKeyNotFound
			}
			return v, nil
		}
	}

	// Check SSTables, Iterate through the loaded SSTables and call Find
	//TODO: Iterate orderly by sequence number
	for i := len(kv.sstables) - 1; i >= 0; i-- {
		val, err := kv.sstables[i].Find(key)
		if err == nil && val != nil {
			return val, nil
		}
	}

	return nil, ErrKeyNotFound
}

func (kv *KVStore) Delete(key []byte) error {
	return kv.Put(key, nil) // Use a nil value as a tombstone
}

func RecoverMemtableFromWAL(walPath string, memtable *memtable.Memtable) error {

	f, err := os.Open(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No WAL file to recover from, this is fine
		}
		return err
	}
	defer f.Close()
	tombStoneByte := make([]byte, 1)
	lenByte := make([]byte, 4)
	for {
		_, err := io.ReadFull(f, tombStoneByte)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		isTombStone := tombStoneByte[0] == 1
		if _, err := io.ReadFull(f, lenByte); err != nil {
			return err
		}
		keyLen := binary.LittleEndian.Uint32(lenByte)
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(f, key); err != nil {
			return err
		}
		if _, err := io.ReadFull(f, lenByte); err != nil {
			return err
		}
		valueLen := binary.LittleEndian.Uint32(lenByte)
		value := make([]byte, valueLen)
		if _, err := io.ReadFull(f, value); err != nil {
			return err
		}
		if isTombStone {
			memtable.Put(key, nil)
		} else {
			memtable.Put(key, value)
		}
	}
	return nil
}

func (kv *KVStore) PrintMemtable() {
	kv.memtable.PrintInOrder()
}

func (kv *KVStore) GetMemtableSize() int64 {
	return kv.memtable.Size()
}

func (kv *KVStore) GetImmutableMemtableSize() int64 {
	size := int64(0)
	for _, m := range kv.immutableMemtable {
		size += m.Size()
	}
	return size
}

func (kv *KVStore) rotateAndFlush() error {
	frozenMemtable := kv.memtable
	kv.immutableMemtable = append(kv.immutableMemtable, frozenMemtable)
	kv.sequenceNumber++
	oldWal := kv.wal
	newWal, err := wal.NewWAL(kv.dir, kv.sequenceNumber)
	if err != nil {
		return err
	}
	kv.wal = newWal
	kv.memtable = memtable.NewMemtable()
	go flushMemtableToSSTableAndDeleteWAL(kv, frozenMemtable, oldWal, kv.sequenceNumber-1)
	return nil

}

func flushMemtableToSSTableAndDeleteWAL(kv *KVStore, memtable *memtable.Memtable, wal *wal.WAL, sequenceNumber int64) {
	ssTablePath := kv.dir + "/sstable/"
	path := filepath.Join(ssTablePath, fmt.Sprintf("data-%04d.sst", sequenceNumber))
	sstable, err := sstable.NewSSTable(path)
	if err != nil {
		fmt.Printf("ERROR creating SSTable %s: %v\n", path, err)
		return
	}
	err = sstable.Write(memtable)
	if err != nil {
		fmt.Printf("ERROR flushing SSTable %s: %v\n", path, err)
		return
	}
	wal.Delete()
	kv.mu.Lock()
	kv.sstables = append(kv.sstables, sstable)
	for i, m := range kv.immutableMemtable {
		if m == memtable {
			// Delete from immutableMemtable
			kv.immutableMemtable = append(kv.immutableMemtable[:i], kv.immutableMemtable[i+1:]...)
			break
		}
	}
	kv.mu.Unlock()
}
