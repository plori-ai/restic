package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/restic"
)

func (f *swFixture) assertLargestFiles(resp treeWriteResponse) {
	f.t.Helper()
	for name, role := range map[string]*treeRole{"head": &resp.Head, "public": resp.Public} {
		if role == nil {
			continue
		}
		if role.Empty {
			if role.LargestFileBytes != nil {
				f.t.Fatalf("%s empty role reports a largest file", name)
			}
			continue
		}
		var largest uint64
		for _, node := range f.flatten(role.Snapshot) {
			if node.Type == data.NodeTypeFile {
				largest = max(largest, node.Size)
			}
		}
		if role.LargestFileBytes == nil || *role.LargestFileBytes != largest {
			f.t.Fatalf("%s largest file: got %v, want %d", name, role.LargestFileBytes, largest)
		}
	}
}

func TestServeWriteLargestFile(t *testing.T) {
	f := newSWFixture(t)
	// Private ancestors exclude every descendant from the public maximum.
	resp := f.edit(restic.ID{}, wr("nested/file", strings.Repeat("x", 17)), wr("nested/.forge/deep/large", strings.Repeat("x", 31)))
	if *resp.Head.LargestFileBytes != 31 || *resp.Public.LargestFileBytes != 17 {
		t.Fatal("nested/private largest sizes differ")
	}
	// A cached tree's maximum must decrease when its largest file shrinks.
	resp = f.edit(mustID(t, resp.Head.Snapshot), wr("nested/.forge/deep/large", "small"))
	if *resp.Head.LargestFileBytes != 17 || *resp.Public.LargestFileBytes != 17 {
		t.Fatal("largest size did not decrease")
	}
	for _, edit := range []testEdit{wr("empty-file", ""), ed(writeEdit{Op: "mkdir", Path: "only-dir"})} {
		resp = f.edit(restic.ID{}, edit)
		if *resp.Head.LargestFileBytes != 0 || *resp.Public.LargestFileBytes != 0 {
			t.Fatal("non-empty zero-byte tree must report zero")
		}
	}
	resp = f.edit(restic.ID{}, ed(writeEdit{Op: "mkdir"}))
	for _, role := range []*treeRole{&resp.Head, resp.Public} {
		raw, err := json.Marshal(role)
		if err != nil || strings.Contains(string(raw), "largest_file_bytes") {
			t.Fatalf("empty role must omit largest_file_bytes: %s %v", raw, err)
		}
	}
}

func TestServeWriteVerifierLargestFile(t *testing.T) {
	f := newSWFixture(t)
	req := f.request(restic.ID{}, wr("file", "public"), wr(".forge/private", "largest private file"))
	resp := f.write(req)
	for _, name := range []string{"head", "public"} {
		for _, absent := range []bool{false, true} {
			bad := resp
			pub := *resp.Public
			bad.Public = &pub
			role := &bad.Head
			if name == "public" {
				role = bad.Public
			}
			wrong := *role.LargestFileBytes + 1
			role.LargestFileBytes = &wrong
			if absent {
				role.LargestFileBytes = nil
			}
			if v := f.verify(req, bad); v.OK || v.Code != "count_mismatch" {
				t.Fatalf("%s absent=%v accepted: %+v", name, absent, v)
			}
		}
	}
	empty := f.request(restic.ID{}, ed(writeEdit{Op: "mkdir"}))
	resp = f.write(empty)
	zero := uint64(0)
	resp.Head.LargestFileBytes = &zero
	if out := f.post("/verify-write", verifyWriteRequest{Version: treeWriteVersion, Request: empty, Result: resp}); out.code != http.StatusBadRequest {
		t.Fatalf("empty head with largest accepted: %d %s", out.code, out.body)
	}
	resp.Head.LargestFileBytes, resp.Public.LargestFileBytes = nil, &zero
	if out := f.post("/verify-write", verifyWriteRequest{Version: treeWriteVersion, Request: empty, Result: resp}); out.code != http.StatusBadRequest {
		t.Fatalf("empty public with largest accepted: %d %s", out.code, out.body)
	}
}
