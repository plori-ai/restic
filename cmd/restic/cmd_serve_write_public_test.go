package main

import (
	"context"
	"encoding/json"
	"github.com/restic/restic/internal/restic"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestServeWritePublicTwinBase(t *testing.T) {
	f := newSWFixture(t)
	// Remove the mixed private/public hard-link fixture for the successful case.
	if err := os.Remove(filepath.Join(f.dir, ".plori-trash/old")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(f.dir, "a/x")); err != nil {
		t.Fatal(err)
	}
	swWrite(t, filepath.Join(f.dir, "c/.forge/nested"), "private at depth", 0644)
	base := f.backup(nil, false)
	req := f.request(base)
	raw, _ := json.Marshal(req)
	var body map[string]any
	_ = json.Unmarshal(raw, &body)
	body["public_twin_of_base"] = true
	before := 0
	if err := f.repo.List(context.TODO(), restic.SnapshotFile, func(restic.ID, int64) error { before++; return nil }); err != nil {
		t.Fatal(err)
	}
	out := f.post("/tree-write", body)
	if out.code != http.StatusOK {
		t.Fatalf("public twin answered %d: %s", out.code, out.body)
	}
	if out.resp.Head.Snapshot != base.String() {
		t.Fatalf("head rewritten: %+v", out.resp.Head)
	}
	if out.resp.Public == nil || out.resp.Public.Empty {
		t.Fatal("public missing")
	}
	after := 0
	if err := f.repo.List(context.TODO(), restic.SnapshotFile, func(restic.ID, int64) error { after++; return nil }); err != nil {
		t.Fatal(err)
	}
	if after != before+1 {
		t.Fatalf("wrote %d snapshots, want one", after-before)
	}
	nodes := f.flatten(out.resp.Public.Snapshot)
	for _, p := range []string{".forge", ".forge/state", "c/.forge", "c/.forge/nested", ".plori-trash"} {
		if nodes[p] != nil {
			t.Fatalf("private node %q leaked", p)
		}
	}
	// Replay the exact new mode through the independent handle.
	verify := map[string]any{"version": treeWriteVersion, "request": body, "result": out.resp}
	v := f.post("/verify-write", verify)
	if v.code != http.StatusOK {
		t.Fatalf("verify answered %d: %s", v.code, v.body)
	}
	var result verifyWriteResponse
	_ = json.Unmarshal([]byte(v.body), &result)
	if !result.OK || !result.HeadManifestValidated {
		t.Fatalf("verify failed: %+v", result)
	}
	req.PublicTwinOfBase = true
	f.assertLargestFiles(out.resp)
	bad := out.resp
	bad.Head.Snapshot = bad.Public.Snapshot
	if v := f.verify(req, bad); v.OK {
		t.Fatal("accepted rewritten head")
	}
	bad = out.resp
	bad.Public = &treeRole{}
	*bad.Public = *out.resp.Public
	bad.Public.Entries++
	if v := f.verify(req, bad); v.OK {
		t.Fatal("accepted forged public count")
	}
}

func TestServeWritePublicTwinEmptyAndHardlinkRefusal(t *testing.T) {
	for _, privateOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "split-hardlink", true: "private-only"}[privateOnly], func(t *testing.T) {
			f := newSWFixture(t)
			if privateOnly {
				entries, err := os.ReadDir(f.dir)
				if err != nil {
					t.Fatal(err)
				}
				for _, e := range entries {
					if err := os.RemoveAll(filepath.Join(f.dir, e.Name())); err != nil {
						t.Fatal(err)
					}
				}
				swWrite(t, filepath.Join(f.dir, ".forge/state"), "private", 0600)
			}
			base := f.backup(nil, false)
			req := f.request(base)
			req.PublicTwinOfBase = true
			out := f.write(req)
			if out.Head.Snapshot != base.String() {
				t.Fatal("head changed")
			}
			if privateOnly {
				if out.Public == nil || !out.Public.Empty {
					t.Fatal("expected empty public")
				}
			} else {
				if out.Public != nil || len(out.IncompleteLinkGroups) != 1 {
					t.Fatalf("split group not refused: %+v", out)
				}
			}
		})
	}
}

func TestServeWritePublicTwinRejectsOtherModes(t *testing.T) {
	f := newSWFixture(t)
	base := f.backup(nil, false)
	for _, change := range []func(*treeWriteRequest){
		func(r *treeWriteRequest) { r.Base = treeSource{Empty: true}; r.Paths = []string{"/scan"} },
		func(r *treeWriteRequest) { r.Edits = []writeEdit{{Op: "mkdir", Path: "new"}} },
		func(r *treeWriteRequest) { r.Merge = &mergePlan{Entries: []mergeEntry{}} },
		func(r *treeWriteRequest) {
			r.Contents = []writeContent{{Length: 0, SHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}}
		},
	} {
		req := f.request(base)
		req.PublicTwinOfBase = true
		change(&req)
		if out := f.post("/tree-write", req); out.code != http.StatusBadRequest {
			t.Fatalf("invalid mode accepted: %s", out.body)
		}
	}
}
