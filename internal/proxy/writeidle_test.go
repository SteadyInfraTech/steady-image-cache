package proxy

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// A client that takes the headers of a big cached blob and then stops reading
// is let go after the idle limit; one that keeps reading, slowly, is not.
func TestStalledClientIsDroppedButASlowOneIsNot(t *testing.T) {
	body := bytes.Repeat([]byte("0123456789abcdef"), 3<<20) // 48 MiB, more than the sockets hold
	d := digestOf(body)
	n := newNode(t, Options{WriteIdle: 400 * time.Millisecond})
	if _, err := n.st.PutVerified(d, strings.NewReader(string(body))); err != nil {
		t.Fatal(err)
	}

	// Stalled: reads a little, then nothing for three times the limit.
	resp, err := http.Get(n.srv.URL + "/v2/lib/app/blobs/" + d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(resp.Body, make([]byte, 1000)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	rest, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err == nil && len(rest)+1000 == len(body) {
		t.Fatal("a client that stopped reading for three idle periods still got the whole blob")
	}

	// Slow: a pause shorter than the limit between reads.
	n = newNode(t, Options{WriteIdle: 5 * time.Second})
	if _, err := n.st.PutVerified(d, strings.NewReader(string(body))); err != nil {
		t.Fatal(err)
	}
	resp, err = http.Get(n.srv.URL + "/v2/lib/app/blobs/" + d)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 4<<20)
	var got int
	for {
		m, err := resp.Body.Read(buf)
		got += m
		if err != nil {
			if err != io.EOF {
				t.Fatalf("slow reader after %d bytes: %v", got, err)
			}
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got != len(body) {
		t.Fatalf("slow reader got %d of %d", got, len(body))
	}
}
