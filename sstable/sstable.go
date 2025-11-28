package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/charithesh16/lsm-kv/memtable"
)

type SSTable struct {
	file *os.File
	path string
}

func NewSSTable(path string) (*SSTable, error) {
	// Create file if it doesn't exist, or open existing file
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &SSTable{file: file, path: path}, nil
}

func (sstable *SSTable) Write(memtable *memtable.Memtable) error {
	// Truncate the file to clear any existing content
	if err := sstable.file.Truncate(0); err != nil {
		return err
	}
	// Seek to the beginning of the file
	if _, err := sstable.file.Seek(0, 0); err != nil {
		return err
	}

	for iter := memtable.Iterator(); iter != nil; iter = iter.Next() {
		key := iter.Key().([]byte)
		value := iter.Value.([]byte)

		//format [tombstone][key length][key][value length][value]
		tombstone := byte(0)
		if value == nil {
			tombstone = byte(1)
		}
		// Write format: [tombstone][key length][key][value length][value]
		keyLen := uint32(len(key))
		valueLen := uint32(len(value))

		buf := make([]byte, 4)

		if _, err := sstable.file.Write([]byte{tombstone}); err != nil {
			return err
		}

		binary.LittleEndian.PutUint32(buf, keyLen)
		if _, err := sstable.file.Write(buf); err != nil {
			return err
		}

		if _, err := sstable.file.Write(key); err != nil {
			return err
		}

		binary.LittleEndian.PutUint32(buf, valueLen)
		if _, err := sstable.file.Write(buf); err != nil {
			return err
		}

		if _, err := sstable.file.Write(value); err != nil {
			return err
		}
	}

	return sstable.file.Sync()
}

func (sstable *SSTable) Find(key []byte) ([]byte, error) {

	// Seek to the beginning of the file
	if _, err := sstable.file.Seek(0, 0); err != nil {
		return nil, err
	}

	buf := make([]byte, 4)

	for {
		// Read tombstone
		tombstone := make([]byte, 1)
		if _, err := io.ReadFull(sstable.file, tombstone); err != nil {
			if err == io.EOF {
				return nil, errors.New("key not found")
			}
			return nil, err
		}

		// Read key length
		if _, err := io.ReadFull(sstable.file, buf); err != nil {
			return nil, err
		}
		keyLen := binary.LittleEndian.Uint32(buf)

		// Read key
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(sstable.file, keyBytes); err != nil {
			return nil, err
		}

		// Read value length
		if _, err := io.ReadFull(sstable.file, buf); err != nil {
			return nil, err
		}
		valueLen := binary.LittleEndian.Uint32(buf)

		// Read value
		valueBytes := make([]byte, valueLen)
		if _, err := io.ReadFull(sstable.file, valueBytes); err != nil {
			return nil, err
		}

		// Check if this is the key we're looking for
		if bytes.Equal(keyBytes, key) {
			// Check if it's a tombstone (deleted)
			if tombstone[0] == 1 {
				return nil, errors.New("key deleted")
			}
			return valueBytes, nil
		}
	}
}

func (sstable *SSTable) Close() error {
	if sstable.file != nil {
		return sstable.file.Close()
	}
	return nil
}
