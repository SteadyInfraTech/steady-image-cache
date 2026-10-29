package proxy

import (
	"context"
	"net/http"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
)

// A download is one fetch of a blob that every client asking for it shares.
// It runs on its own, not in any client's handler, so a client that gives up
// does not stop it and the others. While the content arrives it is written to
// the store's partial file; clients read that file as it grows, which is what
// lets containerd start receiving a layer before the whole layer is here.
//
// The content is only verified at the end. So that a client cannot complete a
// response with content that fails verification, the last byte is held back
// until the store has checked the digest; when the check fails the response
// stops one byte short and the connection is closed, which a client sees as an
// error and retries.
type download struct {
	mu     sync.Mutex
	notify chan struct{} // closed, and replaced, whenever anything below changes
	cur    *attempt      // the most recent source being copied; nil before the first
	done   bool
	res    fetchResult
	auth   string // the Authorization header of the client that started it
}

// attempt is one source being copied into the store. A download may go through
// several: a peer can fail halfway through and the next peer takes over.
type attempt struct {
	path    string
	size    int64
	written int64
	ended   bool
	err     error // why it ended, nil when the blob was stored
}

func newDownload(auth string) *download {
	return &download{notify: make(chan struct{}), auth: auth}
}

// wake must be called with mu held.
func (dl *download) wake() {
	close(dl.notify)
	dl.notify = make(chan struct{})
}

// Begin, Wrote and End implement store.Progress.
func (dl *download) Begin(path string, max int64) {
	dl.mu.Lock()
	dl.cur = &attempt{path: path, size: max}
	dl.wake()
	dl.mu.Unlock()
}

func (dl *download) Wrote(total int64) {
	dl.mu.Lock()
	if dl.cur != nil {
		dl.cur.written = total
	}
	dl.wake()
	dl.mu.Unlock()
}

func (dl *download) End(err error) {
	dl.mu.Lock()
	if dl.cur != nil {
		dl.cur.ended, dl.cur.err = true, err
	}
	dl.wake()
	dl.mu.Unlock()
}

func (dl *download) finish(res fetchResult) {
	dl.mu.Lock()
	dl.done, dl.res = true, res
	dl.wake()
	dl.mu.Unlock()
}

// wait blocks until the download is over and returns its result, or until ctx
// is done, in which case ok is false.
func (dl *download) wait(ctx context.Context) (res fetchResult, ok bool) {
	for {
		dl.mu.Lock()
		done, res, ch := dl.done, dl.res, dl.notify
		dl.mu.Unlock()
		if done {
			return res, true
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return fetchResult{}, false
		}
	}
}

// join returns the download of d that is under way, or starts one that runs
// fn. fn receives the download so that its store can report progress to it.
func (p *Proxy) join(d, auth string, fn func(*download) fetchResult) *download {
	p.dlmu.Lock()
	defer p.dlmu.Unlock()
	if dl, ok := p.downloads[d]; ok {
		return dl
	}
	dl := newDownload(auth)
	p.downloads[d] = dl
	go func() {
		var res fetchResult
		defer func() {
			// A panic here would take the process down; a failed download is
			// reported like any other.
			if v := recover(); v != nil {
				p.warn("panic", "download of %s panicked: %v\n%s", d, v, debug.Stack())
				res = fetchResult{}
			}
			p.dlmu.Lock()
			delete(p.downloads, d)
			p.dlmu.Unlock()
			dl.finish(res)
		}()
		res = fn(dl)
	}()
	return dl
}

// streamable reports whether a request can be answered while the blob is still
// arriving: a plain GET for all of it.
func streamable(r *http.Request) bool {
	return r.Method == http.MethodGet && r.Header.Get("Range") == ""
}

// stream answers r from the download while it runs. It returns false without
// having written anything when there is nothing to stream, because the blob
// was stored already or the download ended without one; the caller then goes
// by wait. It returns true once it has started the response, whether or not
// that response completed.
//
// A peer asking (immediate) is only served from a source that has already
// delivered bytes, and is never made to wait for one. Two nodes that each
// wait for the other to start would otherwise wait for ever: a source that has
// bytes got them from somewhere that had them first, so the chain cannot be a
// loop.
//
// held makes the response wait, however long the caller allows, until the first
// byte has arrived before it sends its headers (a peer asking immediate is
// always served that way). A coordinator's asker bounds the wait for the
// headers, which is then a bound on the wait for the content.
func (p *Proxy) stream(w http.ResponseWriter, r *http.Request, d string, dl *download, immediate, held bool) bool {
	ctx := r.Context()
	var a *attempt
	var f *os.File
	for f == nil {
		dl.mu.Lock()
		cur, done, ch := dl.cur, dl.done, dl.notify
		var usable bool
		if cur != nil && !cur.ended && ((!immediate && !held) || cur.written > 0) {
			a, usable = cur, true
		}
		dl.mu.Unlock()
		if immediate && !usable {
			return false
		}
		if usable {
			var err error
			if f, err = os.Open(a.path); err != nil {
				f = nil // renamed or removed since: look at the state again
			} else {
				break
			}
		}
		if done {
			return false
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return true
		}
	}
	defer f.Close()

	h := w.Header()
	h.Set("Docker-Content-Digest", d)
	h.Set("Content-Type", "application/octet-stream")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Length", strconv.FormatInt(a.size, 10))
	w.WriteHeader(http.StatusOK)
	p.metrics.Streamed.Add(1)
	rc := http.NewResponseController(w)

	buf := make([]byte, 64<<10)
	var off int64
	for off < a.size {
		dl.mu.Lock()
		avail, ended, err, cur, ch, over := a.written, a.ended, a.err, dl.cur, dl.notify, dl.done
		dl.mu.Unlock()
		if err != nil || cur != a && !ended || over && !ended {
			// This source failed or was replaced, or the download is over
			// without having said how this source ended (it panicked).
			break
		}
		if !ended && avail >= a.size {
			avail = a.size - 1 // the last byte waits for the digest check
		}
		if off < avail {
			n := int64(len(buf))
			if avail-off < n {
				n = avail - off
			}
			m, e := f.ReadAt(buf[:n], off)
			if m > 0 {
				wn, we := w.Write(buf[:m])
				if wn > 0 {
					p.servedCounter(r).Add(uint64(wn)) // what was accepted counts, even if the write then failed
				}
				if we != nil {
					return true // the client went away
				}
				off += int64(m)
			}
			if e != nil && m == 0 {
				break
			}
			continue
		}
		if ended { // stored: all of it is in the file
			avail = a.size
			if off >= avail {
				break
			}
			continue
		}
		_ = rc.Flush()
		select {
		case <-ch:
		case <-ctx.Done():
			return true
		}
	}
	if off < a.size {
		// Fewer bytes than Content-Length: the server closes the connection.
		p.metrics.StreamAborts.Add(1)
	}
	return true
}
