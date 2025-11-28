package lsmkv

import (
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/charithesh16/lsm-kv/memtable"
	"github.com/charithesh16/lsm-kv/wal"
)

type KVStore struct {
	memtable *memtable.Memtable
	wal      *wal.WAL
}

var ErrKeyNotFound = errors.New("key not found")

func NewKVStore(dir string) (*KVStore, error) {
	/// check if wal logs is present, if present load into memtable if not create new wal and memtable
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}
	walPath := dir + "/wal.log"
	mem := memtable.NewMemtable()
	if err := RecoverMemtableFromWAL(walPath, mem); err != nil {
		return nil, err
	}
	wal, err := wal.NewWAL(dir)
	if err != nil {
		return nil, err
	}
	return &KVStore{
		memtable: mem,
		wal:      wal,
	}, nil
}

func (kv *KVStore) Put(key, value []byte) error {
	// write to wal first
	err := kv.wal.Write(key, value)
	if err != nil {
		return err
	}
	// write to memtable
	kv.memtable.Put(key, value)
	return nil
}

func (kv *KVStore) Get(key []byte) ([]byte, error) {
	value, ok := kv.memtable.Get(key)
	if !ok || value == nil {
		return nil, ErrKeyNotFound
	}

	return value, nil
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
