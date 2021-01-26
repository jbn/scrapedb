package scrapedb

import (
	"bytes"
	"context"
	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestDB(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	db, err := NewDB(filepath.Join(tmpDir, "blobs"), filepath.Join(tmpDir, "db"))
	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer func() {
		assert.NoError(t, db.Close())
	}()

	// Page doesn't exit.
	page, err := db.GetPage("page", "https://test.com/a")
	assert.EqualError(t, err, badger.ErrKeyNotFound.Error())
	assert.Nil(t, page)

	// There are no pages.
	ks, err := db.ScanPages(ctx, "page")
	for _ = range ks {
		assert.True(t, false)
	}

	// Update pages
	err = db.UpdatePage("page", "https://test.com/a", []byte("a"))
	assert.NoError(t, err)
	got, err := db.GetPage("page", "https://test.com/a")
	assert.NoError(t, err)
	assert.Equal(t, []byte("a"), got.Data)

	err = db.UpdatePage("page", "https://test.com/b", []byte("b"))
	assert.NoError(t, err)
	got, err = db.GetPage("page", "https://test.com/b")
	assert.NoError(t, err)
	assert.Equal(t, []byte("b"), got.Data)

	err = db.UpdatePage("other", "https://test.com/c", []byte("c"))
	assert.NoError(t, err)
	got, err = db.GetPage("other", "https://test.com/c")
	assert.NoError(t, err)
	assert.Equal(t, []byte("c"), got.Data)

	err = db.UpdatePage("other", "https://test.com/c", []byte("cccc"))
	assert.NoError(t, err)
	got, err = db.GetPage("other", "https://test.com/c")
	assert.NoError(t, err)
	assert.Equal(t, []byte("cccc"), got.Data)

	// List pages
	gotPages := map[string]bool{}
	ks, err = db.ScanPages(ctx, "page")
	for k := range ks {
		gotPages[k] = true
	}
	assert.Equal(t, map[string]bool{"https://test.com/a": true, "https://test.com/b": true}, gotPages)

	// Cancelled
	ctxCancelled, cancel := context.WithCancel(ctx)
	cancel()
	ks, err = db.ScanPages(ctxCancelled, "page")
	for _ = range ks {
		assert.True(t, false)
	}
}

func TestBlobOps(t *testing.T) {
	tmpDir := t.TempDir()

	db, err := NewDB(filepath.Join(tmpDir, "blobs"), filepath.Join(tmpDir, "db"))
	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer func() {
		assert.NoError(t, db.Close())
	}()

	fullPath, err := db.BlobPath("pa")
	assert.EqualError(t, err, "filename is too small: 2 < 3")
	assert.Empty(t, fullPath)

	fullPath, err = db.BlobPath("path")
	assert.NoError(t, err)
	assert.Equal(t, filepath.Join(db.blobDir, "p", "a", "t", "path"), fullPath)

	b, err := db.ReadBlob("path")
	assert.Error(t, err)
	assert.Nil(t, b)

	n, err := db.WriteBlob("path", bytes.NewBuffer([]byte("abcd")))
	assert.NoError(t, err)
	assert.Equal(t, int64(4), n)

	b, err = db.ReadBlob("path")
	assert.NoError(t, err)
	assert.Equal(t, "abcd", string(b))
}

func TestPageKey(t *testing.T) {
	assert.Equal(t, []byte("KIND-PATH"), PageKey("KIND", "PATH"))
}
