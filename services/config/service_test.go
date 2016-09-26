package config

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

type SectionA struct {
	Option1 string `toml:"option-1"`
}
type SectionB struct {
	Option2 string `toml:"option-2"`
}

type TestConfig struct {
	SectionA SectionA `toml:"section-a" override:"section-a"`
	SectionB SectionB `toml:"section-b" override:"section-b"`
}

func TestService_handleUpdateRequest(t *testing.T) {
	testCases := []struct {
		body    string
		path    string
		code    int
		expName string
		exp     interface{}
	}{
		{
			body:    `{"set":{"option-1": "new-o1"}}`,
			path:    "/section-a",
			code:    http.StatusNoContent,
			expName: "section-a",
			exp: SectionA{
				Option1: "new-o1",
			},
		},
	}
	testConfig := &TestConfig{
		SectionA: SectionA{
			Option1: "o1",
		},
	}
	updates := make(chan ConfigUpdate, len(testCases))
	service := NewService(testConfig, log.New(os.Stderr, "[handleUpdateRequest] ", log.LstdFlags), updates)
	for _, tc := range testCases {
		r := NewRequest("POST", tc.path, strings.NewReader(tc.body))
		rr := httptest.NewRecorder()

		service.handleUpdateRequest(rr, r)

		// Validate response
		if got, exp := rr.Code, http.StatusNoContent; got != exp {
			t.Errorf("unexpected code: got %d exp %d.\nBody:\n%s", got, exp, rr.Body.String())
		}

		// Validate we got the update over the chan
		timer := time.NewTimer(10 * time.Millisecond)
		defer timer.Stop()
		select {
		case cu := <-updates:
			if got, exp := cu.Name, tc.expName; got != exp {
				t.Errorf("unexpected config update Name: got %s exp %s", got, exp)
			}
			if !reflect.DeepEqual(cu.NewConfig, tc.exp) {
				t.Errorf("unexpected new config: got %v exp %v", cu.NewConfig, tc.exp)
			}
		case <-timer.C:
			t.Fatal("expected to get config update")
		}
	}
}

func NewRequest(method, target string, body io.Reader) *http.Request {
	if method == "" {
		method = "GET"
	}
	req, err := http.ReadRequest(bufio.NewReader(strings.NewReader(method + " " + target + " HTTP/1.0\r\n\r\n")))
	if err != nil {
		panic("invalid NewRequest arguments; " + err.Error())
	}

	// HTTP/1.0 was used above to avoid needing a Host field. Change it to 1.1 here.
	req.Proto = "HTTP/1.1"
	req.ProtoMinor = 1
	req.Close = false

	if body != nil {
		switch v := body.(type) {
		case *bytes.Buffer:
			req.ContentLength = int64(v.Len())
		case *bytes.Reader:
			req.ContentLength = int64(v.Len())
		case *strings.Reader:
			req.ContentLength = int64(v.Len())
		default:
			req.ContentLength = -1
		}
		if rc, ok := body.(io.ReadCloser); ok {
			req.Body = rc
		} else {
			req.Body = ioutil.NopCloser(body)
		}
	}

	// 192.0.2.0/24 is "TEST-NET" in RFC 5737 for use solely in
	// documentation and example source code and should not be
	// used publicly.
	req.RemoteAddr = "192.0.2.1:1234"

	if req.Host == "" {
		req.Host = "example.com"
	}

	if strings.HasPrefix(target, "https://") {
		req.TLS = &tls.ConnectionState{
			Version:           tls.VersionTLS12,
			HandshakeComplete: true,
			ServerName:        req.Host,
		}
	}

	return req
}
