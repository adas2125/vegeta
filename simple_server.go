//go:build ignore
// +build ignore

package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	delay := flag.Duration("delay", 0, "response delay (e.g. 50ms, 200ms)")
	flag.Parse()

	file, err := os.Create("output.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	var totalCount uint64
	var mu sync.Mutex

	// add header to the CSV file
	_ = writer.Write([]string{"timestamp", "total_requests"})
	writer.Flush()

	// ---- periodic logging ----
	go func() {
		for {
			time.Sleep(1 * time.Second)
			current := atomic.LoadUint64(&totalCount)

			// write to CSV file, the number of requests received in the last second
			mu.Lock()
			_ = writer.Write([]string{time.Now().Format(time.RFC3339), fmt.Sprintf("%d", current)})
			writer.Flush()
			mu.Unlock()
		}
	}()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		if *delay > 0 {
			time.Sleep(*delay)
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		atomic.AddUint64(&totalCount, 1)

	})

	fmt.Println("server listening on", *addr)
	_ = http.ListenAndServe(*addr, nil)
}
