package scrapedb

import (
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"golang.org/x/net/proxy"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"runtime"
	"time"
)

type Spider struct {
	client        *http.Client
	db            *DB
	sleepInterval time.Duration
	userAgent     string
}

type SpiderOpt func(s *Spider)

func NewSpider(db *DB, opts ...SpiderOpt) *Spider {
	s := &Spider{
		client:        &http.Client{},
		db:            db,
		sleepInterval: time.Second,
		userAgent:     "ScrapeDB",
	}

	for _, f := range opts {
		f(s)
	}

	return s
}

func (s *Spider) GetSleepInterval() time.Duration {
	return s.sleepInterval
}

func WithClient(client *http.Client) SpiderOpt {
	return func(s *Spider) {
		s.client = client
	}
}

func WithSOCKS(proxyHost string) SpiderOpt {
	return func(s *Spider) {
		baseDialer := &net.Dialer{}
		dialSocksProxy, err := proxy.SOCKS5("tcp", proxyHost, nil, baseDialer)
		if err != nil {
			log.Fatalf("Error creating proxy: %s", err)
		}

		contextDialer, ok := dialSocksProxy.(proxy.ContextDialer)
		if !ok {
			log.Fatalf("Non DialContext type assertion: %T", dialSocksProxy)
		}

		s.client = &http.Client{
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				DialContext:           contextDialer.DialContext,
				MaxIdleConns:          10,
				IdleConnTimeout:       60 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				MaxIdleConnsPerHost:   runtime.GOMAXPROCS(0) + 1,
			},
		}
	}
}

func WithUserAgent(userAgent string) SpiderOpt {
	return func(s *Spider) {
		s.userAgent = userAgent
	}
}

func WithSleepTime(duration time.Duration) SpiderOpt {
	return func(s *Spider) {
		s.sleepInterval = duration
	}
}

func (s *Spider) RequestPage(kind, url string, staleAfter time.Duration) ([]byte, bool, error) {
	page, err := s.db.GetPage(kind, url)
	if err != nil {
		if err != badger.ErrKeyNotFound {
			return nil, false, err
		}
	} else {
		// Valid cache.
		if page.FetchedAt.Add(staleAfter).After(time.Now().UTC()) {
			return page.Data, true, nil
		}
	}

	resp, err := s.getRequest(url)
	if err != nil {
		return nil, false, err
	}

	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, false, err
	}

	err = s.db.UpdatePage(kind, url, data)
	if err != nil {
		return nil, false, err
	}

	return data, false, nil
}

func (s *Spider) RequestBlob(kind, url string, staleAfter time.Duration) (int64, bool, error) {
	page, err := s.db.GetPage(kind, url)
	if err != nil {
		if err != badger.ErrKeyNotFound {
			return -1, false, err
		}
	} else {
		// Valid cache.
		if page.FetchedAt.Add(staleAfter).After(time.Now().UTC()) {
			return 0, true, nil
		}
	}

	filename := filepath.Base(url)
	if len(filename) < 5 {
		return -1, false, fmt.Errorf("'%s' is too short", filename)
	}

	resp, err := s.getRequest(url)
	if err != nil {
		return -1, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return -1, false, fmt.Errorf("error getting %s: %s", url, resp.Status)
	}

	nWritten, err := s.db.WriteBlob(filename, resp.Body)
	if err != nil {
		return -1, false, fmt.Errorf("error reading %s: %w", url, err)
	}

	err = s.db.UpdatePage(kind, url, []byte(fmt.Sprintf("%d", nWritten)))
	if err != nil {
		return -1, false, err
	}

	return nWritten, false, nil
}

func (s *Spider) getRequest(url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", s.userAgent)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
