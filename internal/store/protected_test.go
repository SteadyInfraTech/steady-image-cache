package store

import (
	"bytes"
	"testing"
)

func TestProtectedMarkSurvivesARestartAndGoesWithTheBlob(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	b := []byte("blob")
	d := digestOf(b)
	if _, err := s.PutVerified(d, bytes.NewReader(b)); err != nil {
		t.Fatal(err)
	}
	if s.IsProtected(d) {
		t.Fatal("marked before anything marked it")
	}
	if err := s.MarkProtected(d); err != nil {
		t.Fatal(err)
	}
	s2, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if !s2.IsProtected(d) {
		t.Fatal("the mark did not survive a restart")
	}
	s2.forget(d)
	if s2.IsProtected(d) {
		t.Fatal("the mark outlived the blob")
	}
	if s2.IsProtected("not a digest") || s2.MarkProtected("not a digest") == nil {
		t.Fatal("a bad digest was accepted")
	}
}
