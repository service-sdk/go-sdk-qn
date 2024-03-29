package operation

import (
	"net"
	"net/http"
	"strconv"
	"strings"
)

// Deprecated:
func StartSimulateErrorServer(_ *Config) {
	httpCode := ":10801"
	errSocket := ":10082"
	elog.Info("start error simulate")
	go simulateConnectionError(errSocket)
	simulateHttpCode(httpCode)
}

func handleConnection(conn net.Conn) {
	elog.Infof("close connection: remoteAddr=%s", conn.RemoteAddr())
	conn.Close()
}

func simulateConnectionError(addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		elog.Infof("listen failed: err=%s", err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			elog.Infof("accept error: err=%s", err)
		}
		go handleConnection(conn)
	}
}

type debug struct {
}

func (d *debug) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	seps := strings.Split(strings.TrimPrefix(path, "/"), "/")
	code, err := strconv.ParseUint(seps[0], 10, 64)
	elog.Info("request is ", path)
	if err != nil {
		elog.Infof("parse code failed: path=%s, err=%s", seps[0], err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Length", "32")
	w.WriteHeader(int(code))
}

func simulateHttpCode(addr string) {
	http.ListenAndServe(addr, &debug{})
}
