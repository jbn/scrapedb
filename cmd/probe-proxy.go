package main

import (
	"bytes"
	"fmt"
	"github.com/jbn/scrapedb"
	"log"
	"os"
	"path/filepath"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatalln("Usage: proxy-probe proxy")
	}
	proxyHost := os.Args[1]

	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("probe-proxy-%d", time.Now().Unix()))
	err := os.Mkdir(tmpDir, 0777)
	if err != nil {
		log.Fatalln(err)
	}
	defer os.RemoveAll(tmpDir)

	db, err := scrapedb.NewDB(tmpDir, tmpDir)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	spider := scrapedb.NewSpider(db, scrapedb.WithSOCKS(proxyHost))
	data, _,  err := spider.RequestPage("test", "https://google.com/", time.Second)
	if err != nil {
		log.Fatalln(err)
	}

	// TODO: this is a shitty test given analytics
	if !bytes.Contains(data, []byte("google")) {
		log.Fatalln(string(data))
	}

	fmt.Println("ok")
}
