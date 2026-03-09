package main

// We need to run
// openssl req -x509 -newkey rsa:4096 \
//   -keyout server.key \
//   -out server.crt \
//   -days 365 \
//   -nodes \
//   -subj "/CN=localhost"

import (
	"flag"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

func main() {

	addr := flag.String("addr", ":8443", "listen address")
	delay := flag.Duration("delay", 0, "response delay (e.g. 200ms, 1s)")
	flag.Parse()

	var total uint64

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// fmt.Println("protocol:", r.Proto)
		if *delay > 0 {
			time.Sleep(*delay)
		}

		atomic.AddUint64(&total, 1)

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// periodic logging
	go func() {
		for {
			time.Sleep(time.Second)
			// fmt.Printf("total requests: %d\n", atomic.LoadUint64(&total))
		}
	}()

	fmt.Println("HTTP/2 server listening on", *addr)

	// Requires TLS for HTTP/2
	err := http.ListenAndServeTLS(*addr, "server.crt", "server.key", nil)
	if err != nil {
		panic(err)
	}
}
