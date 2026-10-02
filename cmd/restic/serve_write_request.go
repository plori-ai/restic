package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path"
	"strings"
	"time"

	"github.com/restic/restic/internal/restic"
)

// errUnsupportedVersion refuses a request for another protocol version.
var errUnsupportedVersion = errors.New("unsupported protocol version")

// treeSource names the exact tree a request starts from: a snapshot (full ID)
// or the empty tree. Exactly one is set.
type treeSource struct {
	Snapshot string `json:"snapshot,omitempty"`
	Empty    bool   `json:"empty,omitempty"`
}

func (t treeSource) validate(field string) error {
	if t.Empty == (t.Snapshot != "") {
		return invalidf("%s: exactly one of snapshot and empty", field)
	}
	if t.Snapshot != "" {
		if _, err := restic.ParseID(t.Snapshot); err != nil || strings.ToLower(t.Snapshot) != t.Snapshot {
			return invalidf("%s: snapshot must be a full lower-case ID", field)
		}
	}
	return nil
}

// writeEdit is one Files edit. ExpectedETag nil skips the precondition; 0
// requires that the path does not exist (write only).
type writeEdit struct {
	Op           string  `json:"op"`
	Path         string  `json:"path,omitempty"`
	To           string  `json:"to,omitempty"`
	Handle       string  `json:"handle,omitempty"`
	Content      *int    `json:"content,omitempty"`
	Mode         uint32  `json:"mode,omitempty"`
	ExpectedETag *uint64 `json:"expected_etag,omitempty"`
}

// mergeEntry is one name of a merge result (the merge manifest of
// plori-runtime's workspacerev/merge.Entry).
type mergeEntry struct {
	Path      string `json:"path"`
	Kind      string `json:"kind"`
	Mode      uint32 `json:"mode"`
	Size      int64  `json:"size"`
	Digest    string `json:"digest,omitempty"`
	Target    string `json:"target,omitempty"`
	LinkGroup string `json:"link_group,omitempty"`
}

type mergePlan struct {
	Sources []treeSource `json:"sources,omitempty"`
	Entries []mergeEntry `json:"entries"`
}

// writeContent is one content item of a request. Data is inline (base64 in
// JSON) in a tree-write request and absent in a verify-write request.
type writeContent struct {
	Length int64  `json:"length"`
	SHA256 string `json:"sha256"`
	Data   []byte `json:"data,omitempty"`
}

type treeWriteRequest struct {
	Version int        `json:"version"`
	Base    treeSource `json:"base"`
	// Time is the mtime/ctime of created and changed nodes (RFC 3339 with
	// nanoseconds). A repeated request writes the same trees.
	Time  string     `json:"time"`
	Owner *[2]uint32 `json:"owner"`
	// Hostname, Tags and PublicTags are the snapshot metadata; the twin has
	// Tags followed by PublicTags.
	Hostname   string         `json:"hostname"`
	Tags       []string       `json:"tags"`
	PublicTags []string       `json:"public_tags,omitempty"`
	Paths      []string       `json:"paths,omitempty"`
	Edits      []writeEdit    `json:"edits,omitempty"`
	Merge      *mergePlan     `json:"merge,omitempty"`
	Contents   []writeContent `json:"contents,omitempty"`

	now time.Time
}

type prepareWriteRequest struct {
	Version int        `json:"version"`
	Base    treeSource `json:"base"`
}

const (
	maxEdits    = 1024
	maxContents = 4096
	maxTags     = 64
)

func checkName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return invalidf("name %q", name)
	}
	return nil
}

func checkTag(tag string) error {
	if tag == "" || len(tag) > 1024 || strings.ContainsAny(tag, ",\x00\n") {
		return invalidf("tag %q", tag)
	}
	return nil
}

// validate checks the request shape. Path, ETag and existence checks need the
// tree and run later; nothing is written before all of them passed.
func (r *treeWriteRequest) validate(cfg serveWriteConfig, inline bool) error {
	if r.Version != treeWriteVersion {
		return errUnsupportedVersion
	}
	if err := r.Base.validate("base"); err != nil {
		return err
	}
	now, err := time.Parse(time.RFC3339Nano, r.Time)
	if err != nil || now.IsZero() {
		return invalidf("time %q", r.Time)
	}
	r.now = now
	if r.Owner == nil {
		return invalidf("owner is required")
	}
	if r.Hostname == "" || strings.ContainsRune(r.Hostname, 0) {
		return invalidf("hostname is required")
	}
	if len(r.Tags) == 0 || len(r.Tags)+len(r.PublicTags) > maxTags {
		return invalidf("tags: 1 to %d", maxTags)
	}
	for _, t := range append(append([]string(nil), r.Tags...), r.PublicTags...) {
		if err := checkTag(t); err != nil {
			return err
		}
	}
	if r.Base.Empty != (len(r.Paths) > 0) {
		return invalidf("paths are required with an empty base and taken from a base snapshot otherwise")
	}
	for _, p := range r.Paths {
		if !path.IsAbs(p) || path.Clean(p) != p || strings.ContainsRune(p, 0) {
			return invalidf("path %q", p)
		}
	}
	if len(r.Contents) > maxContents {
		return invalidf("more than %d contents", maxContents)
	}
	for i, c := range r.Contents {
		if c.Length < 0 || len(c.SHA256) != 64 || strings.ToLower(c.SHA256) != c.SHA256 {
			return invalidf("content %d descriptor", i)
		}
		if _, err := hex.DecodeString(c.SHA256); err != nil {
			return invalidf("content %d sha256", i)
		}
		if !inline {
			if c.Data != nil {
				return invalidf("content %d: verify-write carries no data", i)
			}
			continue
		}
		sum := sha256.Sum256(c.Data)
		if int64(len(c.Data)) != c.Length || hex.EncodeToString(sum[:]) != c.SHA256 {
			return invalidf("content %d does not match its length and sha256", i)
		}
	}
	if (len(r.Edits) > 0) == (r.Merge != nil) {
		return invalidf("exactly one of edits and merge")
	}
	if r.Merge != nil {
		for i, src := range r.Merge.Sources {
			if err := src.validate("merge.sources"); err != nil {
				return invalidf("source %d: %v", i, err)
			}
		}
		return nil
	}
	if len(r.Edits) > maxEdits {
		return invalidf("more than %d edits", maxEdits)
	}
	for i := range r.Edits {
		if err := r.Edits[i].validate(cfg, len(r.Contents)); err != nil {
			return invalidf("edit %d: %v", i, err)
		}
	}
	return nil
}

// validate checks the fields of one edit; each op takes only its own fields.
func (e *writeEdit) validate(cfg serveWriteConfig, contents int) error {
	want := map[string]bool{}
	switch e.Op {
	case "write":
		want = map[string]bool{"path": true, "content": true, "mode?": true, "etag?": true}
		if e.Content == nil || *e.Content < 0 || *e.Content >= contents {
			return invalidf("content index")
		}
		if e.Mode&^0777 != 0 {
			return invalidf("mode")
		}
	case "mkdir":
		want = map[string]bool{"path?": true, "mode?": true}
		if e.Mode&^0777 != 0 {
			return invalidf("mode")
		}
	case "rename":
		want = map[string]bool{"path": true, "to": true, "etag?": true}
	case "chmod":
		want = map[string]bool{"path": true, "mode?": true, "etag?": true}
		if e.Mode&^07777 != 0 {
			return invalidf("mode")
		}
	case "trash":
		want = map[string]bool{"path": true, "handle": true, "etag?": true}
	case "restore":
		want = map[string]bool{"path": true, "handle": true}
	case "empty-trash":
	default:
		return invalidf("op %q", e.Op)
	}
	switch e.Op {
	case "trash", "restore", "empty-trash":
		if cfg.trashDir == "" {
			return invalidf("op %q needs --trash-dir", e.Op)
		}
	}
	has := map[string]bool{"path": e.Path != "", "to": e.To != "", "handle": e.Handle != "", "content": e.Content != nil,
		"mode": e.Mode != 0, "etag": e.ExpectedETag != nil}
	for field, set := range has {
		if set && !want[field] && !want[field+"?"] {
			return invalidf("field %q is not used by %q", field, e.Op)
		}
		if !set && want[field] {
			return invalidf("field %q is required by %q", field, e.Op)
		}
	}
	if e.Handle != "" {
		if err := checkName(e.Handle); err != nil {
			return err
		}
	}
	return nil
}

type verifyWriteRequest struct {
	Version int               `json:"version"`
	Request treeWriteRequest  `json:"request"`
	Result  treeWriteResponse `json:"result"`
}

func (v *verifyWriteRequest) validate(cfg serveWriteConfig) error {
	if v.Version != treeWriteVersion {
		return errUnsupportedVersion
	}
	if err := v.Request.validate(cfg, false); err != nil {
		return err
	}
	if len(v.Result.Contents) != len(v.Request.Contents) {
		return invalidf("result.contents must describe every request content")
	}
	for i, c := range v.Result.Contents {
		for _, id := range c.IDs {
			if _, err := restic.ParseID(id); err != nil {
				return invalidf("result.contents %d: blob ID %q", i, id)
			}
		}
	}
	if err := v.Result.Head.validate("result.head"); err != nil {
		return err
	}
	if v.Result.Public != nil {
		return v.Result.Public.validate("result.public")
	}
	return nil
}

func (r *treeRole) validate(field string) error {
	if r.Empty {
		if r.Snapshot != "" || r.Tree != "" || r.Entries != 0 || r.LogicalBytes != 0 {
			return invalidf("%s: an empty role has no snapshot, tree or counts", field)
		}
		return nil
	}
	for _, id := range []string{r.Snapshot, r.Tree} {
		if _, err := restic.ParseID(id); err != nil {
			return invalidf("%s: IDs", field)
		}
	}
	return nil
}
