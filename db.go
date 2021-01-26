package scrapedb

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type DB struct {
	conn    *badger.DB
	blobDir string
}

type Page struct {
	FetchedAt time.Time `json:"fetchedAt"`
	Data      []byte    `json:"data"`
}

func NewDB(blobDir, badgerPath string) (*DB, error) {
	db, err := badger.Open(badger.DefaultOptions(badgerPath))
	if err != nil {
		return nil, err
	}
	return &DB{
		conn:    db,
		blobDir: blobDir,
	}, nil
}

func (db *DB) Close() error {
	return db.conn.Close()
}

func PageKey(kind, path string) []byte {
	return []byte(fmt.Sprintf("%s-%s", kind, path))
}

func (db *DB) GetPage(kind, path string) (*Page, error) {
	var page Page
	err := db.GetCompressedJSON(PageKey(kind, path), &page)
	if err != nil {
		return nil, err
	}
	return &page, nil
}

func (db *DB) UpdatePage(kind, path string, data []byte) error {
	return db.conn.Update(func(txn *badger.Txn) error {
		b, err := compressedJSON(&Page{
			FetchedAt: time.Now().UTC(),
			Data:      data,
		})
		if err != nil {
			return err
		}
		return txn.Set(PageKey(kind, path), b)
	})
}

func (db *DB) ScanPages(ctx context.Context, kind string) (<-chan string, error) {
	ch := make(chan string)

	go func() {
		defer close(ch)

		prefix := kind + "-"
		completePrefix := []byte(prefix)

		err := db.conn.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()

			for it.Seek(completePrefix); it.ValidForPrefix(completePrefix); it.Next() {
				cancelled := false
				select {
				case <-ctx.Done():
					cancelled = true
					break
				default:
				}
				if cancelled {
					break
				}
				item := it.Item()
				k := string(item.Key())
				ch <- strings.TrimPrefix(k, prefix)
			}
			return nil
		})

		if err != nil {
			log.Printf("error for query '%s': %s", kind, err)
		}
	}()

	return ch, nil
}

func (db *DB) GetCompressedJSON(key []byte, obj interface{}) error {
	return db.conn.View(func(txn *badger.Txn) error {
		return getCompressedJSON(txn, key, obj)
	})
}

func (db *DB) BlobPath(filename string) (string, error) {
	if k := len(filename); k < 3 {
		return "", fmt.Errorf("filename is too small: %d < 3", k)
	}

	parts := []string{db.blobDir}
	for i, c := range filename {
		parts = append(parts, fmt.Sprintf("%c", c))
		if i == 2 {
			break
		}
	}
	dirPath := filepath.Join(parts...)
	err := os.MkdirAll(dirPath, 0775)
	if err != nil {
		return "", err
	}
	return filepath.Join(dirPath, filename), nil
}

func (db *DB) ReadBlob(filename string) ([]byte, error) {
	fullPath, err := db.BlobPath(filename)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadFile(fullPath)
}

func (db *DB) WriteBlob(filename string, r io.Reader) (int64, error) {
	fullPath, err := db.BlobPath(filename)
	if err != nil {
		return -1, err
	}

	fp, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return -1, err
	}

	return io.Copy(fp, r)
}

// =====================================================================================================================

func getCompressedJSON(txn *badger.Txn, key []byte, obj interface{}) error {
	item, err := txn.Get(key)
	if err != nil {
		return err
	}

	return item.Value(func(val []byte) error {
		r, err := gzip.NewReader(bytes.NewBuffer(val))
		if err != nil {
			return err
		}
		defer r.Close()

		return json.NewDecoder(r).Decode(obj)
	})
}

func compressedJSON(obj interface{}) ([]byte, error) {
	b, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, err = w.Write(b)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
