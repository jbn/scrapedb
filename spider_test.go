package scrapedb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestNewSpider(t *testing.T) {
	tmpDir := t.TempDir()

	db, err := NewDB(filepath.Join(tmpDir, "blobs"), filepath.Join(tmpDir, "db"))
	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer func() {
		assert.NoError(t, db.Close())
	}()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, r.RequestURI)
	}))
	defer ts.Close()

	spider := NewSpider(db)

	url := ts.URL + "/my-page"

	got, cached, err := spider.RequestPage("page", url, time.Minute)
	assert.NoError(t, err)
	assert.False(t, cached)
	assert.Equal(t, "/my-page", string(got))

	got, cached, err = spider.RequestPage("page", url, time.Minute)
	assert.NoError(t, err)
	assert.True(t, cached)
	assert.Equal(t, "/my-page", string(got))

	url = ts.URL + "/my-blob"

	nRead, cached, err := spider.RequestBlob("page", url, time.Minute)
	assert.NoError(t, err)
	assert.False(t, cached)
	assert.Equal(t, int64(8), nRead)

	nRead, cached, err = spider.RequestBlob("page", url, time.Minute)
	assert.NoError(t, err)
	assert.True(t, cached)
	assert.Equal(t, int64(0), nRead)
}

func TestConfigOptions(t *testing.T) {
	spider := NewSpider(nil, WithSleepTime(time.Hour), WithUserAgent("MyUserAgent"))
	assert.NotNil(t, spider)
	assert.Equal(t, time.Hour, spider.sleepInterval)
	assert.Equal(t, time.Hour, spider.GetSleepInterval())
	assert.Equal(t, "MyUserAgent", spider.userAgent)
}
