package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type WAL struct {
	file           *os.File
	mu             sync.Mutex
	path           string
	sequenceNumber int64
}

func NewWAL(dir string, sequenceNumber int64) (*WAL, error) {
	// Ensure the directory exists
	walDir := filepath.Join(dir, "wal")
	err := os.MkdirAll(walDir, 0755)
	if err != nil {
		return nil, err
	}

	path := filepath.Join(walDir, fmt.Sprintf("wal-%04d.log", sequenceNumber))
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{file: f, path: path, sequenceNumber: sequenceNumber}, nil
}

func (w *WAL) Write(key, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// keep this to track deleted records, 0 is present, 1 is deleted
	tombstone := byte(0)
	if value == nil {
		tombstone = byte(1)
	}

	// Write format: [tombstone][key length][key][value length][value]
	keyLen := uint32(len(key))
	valueLen := uint32(len(value))

	buf := make([]byte, 4)

	if _, err := w.file.Write([]byte{tombstone}); err != nil {
		return err
	}

	binary.LittleEndian.PutUint32(buf, keyLen)
	if _, err := w.file.Write(buf); err != nil {
		return err
	}

	if _, err := w.file.Write(key); err != nil {
		return err
	}

	binary.LittleEndian.PutUint32(buf, valueLen)
	if _, err := w.file.Write(buf); err != nil {
		return err
	}

	if _, err := w.file.Write(value); err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

func (w *WAL) Delete() error {
	w.Close()
	return os.Remove(w.path)
}
