package resticrev

// Overlay-only test for the PLO-1198 B prototype (not in the repository): it
// reads three revisions with the pinned reader, runs the real merge.Merge and
// writes the merge result for serve-write's /merge-write, and a reference
// snapshot from the platform materializer (Repository.Write).

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/plori/plori-runtime/services/storage-worker/internal/workspacerev/merge"
)

func TestProtoMergeFixture(t *testing.T) {
	repo := os.Getenv("PROTO_REPO")
	if repo == "" {
		t.Skip("PROTO_REPO unset")
	}
	env := []string{"RESTIC_PASSWORD=" + os.Getenv("RESTIC_PASSWORD")}
	for _, k := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"} {
		if v := os.Getenv(k); v != "" {
			env = append(env, k+"="+v)
		}
	}
	r := Repository{Binary: os.Getenv("PROTO_BIN"), Repo: repo, Env: env}
	ctx := context.Background()
	read := func(k string) Snapshot {
		s, err := r.Read(ctx, os.Getenv(k))
		if err != nil {
			t.Fatalf("read %s: %v", k, err)
		}
		return s
	}
	base, current, incoming := read("PROTO_BASE"), read("PROTO_CURRENT"), read("PROTO_INCOMING")
	res, err := merge.Merge(ctx, merge.Input{Base: base.Revision, Current: current.Revision, Incoming: incoming.Revision},
		r.Blobs(ctx, base, current, incoming), merge.Options{IncludeRootVenv: true})
	if err != nil {
		t.Fatal(err)
	}
	out, err := json.MarshalIndent(map[string]any{"entries": res.Merged, "blobs": res.Blobs, "changes": res.Changes, "conflicts": res.Conflicts}, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(os.Getenv("PROTO_OUT"), out, 0o600); err != nil {
		t.Fatal(err)
	}
	ref, err := r.Write(ctx, res, current, base, incoming)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(os.Getenv("PROTO_OUT")+".ref", []byte(ref.ID), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("merged %d entries, %d generated blobs, %d changes, %d conflicts, reference %s", len(res.Merged), len(res.Blobs), len(res.Changes), len(res.Conflicts), ref.ID)
}
