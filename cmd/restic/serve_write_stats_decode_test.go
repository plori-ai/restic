package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/restic"
)

func TestDecodeStatNodesFullFieldValidation(t *testing.T) {
	node := data.Node{Name: string([]byte{'a', 0xff}), Type: data.NodeTypeSymlink, Mode: 0777, ModTime: time.Now(), LinkTarget: string([]byte{0xff, 'b'}), ExtendedAttributes: []data.ExtendedAttribute{{Name: "user.fixture", Value: []byte{0, 0xff}}}}
	encoded, err := json.Marshal(node)
	if err != nil {
		t.Fatal(err)
	}
	good := append(append([]byte(`{"before":true,"nodes":[`), encoded...), []byte(`],"after":{"unknown":true}}`)...)
	got, err := decodeStatNodes(good)
	if err != nil || len(got) != 1 || got[0].Name != node.Name {
		t.Fatalf("lossless name: %+v %v", got, err)
	}
	for _, malformed := range []string{
		`{"nodes":null}`, `{"nodes":{}}`, `{"nodes":[{"name":"a","mtime":"invalid"}]}`,
		`{"nodes":[{"name":"a","mode":"bad"}]}`, `{"nodes":[{"name":"a","uid":-1}]}`,
		`{"nodes":[{"name":"a","linktarget_raw":"%%%"}]}`,
		`{"nodes":[{"name":"a","extended_attributes":[{"name":"user.x","value":"%%%"}]}]}`,
		`{"nodes":[{"name":"a","subtree":"bad"}]}`, `{"nodes":[{"name":"a","content":["bad"]}]}`,
	} {
		t.Run(malformed, func(t *testing.T) {
			iterator, oldErr := data.NewTreeNodeIterator(bytes.NewBufferString(malformed))
			if oldErr == nil {
				for item := range iterator {
					if item.Error != nil {
						oldErr = item.Error
						break
					}
				}
			}
			if oldErr == nil {
				t.Fatal("old decoder unexpectedly accepted fixture")
			}
			if _, err := decodeStatNodes([]byte(malformed)); err == nil {
				t.Fatal("batch decoder weakened field validation")
			}
		})
	}
}

func TestWorkspaceHeadManifestReceipt(t *testing.T) {
	root := restic.Hash([]byte("fixture"))
	for _, tc := range []struct {
		name  string
		stats *treeStats
		want  bool
	}{
		{"valid", &treeStats{workspaceManifest: true}, true},
		{"unsupported kind or invalid name", &treeStats{workspaceManifest: false}, false},
		{"external alias", &treeStats{workspaceManifest: true, linked: []linkName{{key: inodeKey{1, 1}, links: 2, rel: "a"}}}, false},
		{"inconsistent group", &treeStats{workspaceManifest: true, linked: []linkName{{key: inodeKey{1, 1}, links: 2, rel: "a"}, {key: inodeKey{1, 1}, links: 3, rel: "b"}}}, false},
		{"complete group", &treeStats{workspaceManifest: true, linked: []linkName{{key: inodeKey{1, 1}, links: 2, rel: "a"}, {key: inodeKey{1, 1}, links: 2, rel: "b"}}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &serveWriteHandler{stats: map[restic.ID]*treeStats{root: tc.stats}}
			if got := s.workspaceHeadManifest(treeRole{Tree: root.String()}); got != tc.want {
				t.Fatalf("receipt=%v want=%v", got, tc.want)
			}
		})
	}
	if (&serveWriteHandler{}).workspaceHeadManifest(treeRole{Tree: strings.Repeat("f", 64)}) {
		t.Fatal("certified an unread root")
	}
}

func TestWorkspaceManifestNodeChecks(t *testing.T) {
	for _, tc := range []struct {
		name  string
		nodes []statNode
		want  bool
	}{
		{"regular file", []statNode{{Name: "file", Type: data.NodeTypeFile}}, true},
		{"raw byte name", []statNode{{Name: string([]byte{'f', 0xff}), Type: data.NodeTypeFile}}, true},
		{"duplicate", []statNode{{Name: "file", Type: data.NodeTypeFile}, {Name: "file", Type: data.NodeTypeSymlink}}, false},
		{"slash", []statNode{{Name: "a/b", Type: data.NodeTypeFile}}, false},
		{"dot", []statNode{{Name: ".", Type: data.NodeTypeFile}}, false},
		{"nul", []statNode{{Name: "a\x00b", Type: data.NodeTypeFile}}, false},
		{"error", []statNode{{Name: "file", Type: data.NodeTypeFile, Error: "unreadable"}}, false},
		{"oversize", []statNode{{Name: "file", Type: data.NodeTypeFile, Size: 1 << 63}}, false},
		{"fifo", []statNode{{Name: "file", Type: data.NodeTypeFifo}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &serveWriteHandler{stats: map[restic.ID]*treeStats{}, cfg: testServeWriteConfig()}
			st, err := s.rememberStats(context.Background(), restic.Hash([]byte(tc.name)), tc.nodes)
			if err != nil || st.workspaceManifest != tc.want {
				t.Fatalf("manifest=%+v err=%v", st, err)
			}
		})
	}
}

func TestWorkspaceManifestDepthBoundary(t *testing.T) {
	root, child := restic.Hash([]byte("root")), restic.Hash([]byte("child"))
	for _, height := range []int{127, 128} {
		s := &serveWriteHandler{stats: map[restic.ID]*treeStats{child: {workspaceManifest: true, height: height}}, cfg: testServeWriteConfig()}
		st, err := s.rememberStats(context.Background(), root, []statNode{{Name: "dir", Type: data.NodeTypeDir, Subtree: &child}})
		if err != nil || st.workspaceManifest != (height == 127) {
			t.Fatalf("height %d: receipt=%+v err=%v", height+1, st, err)
		}
	}
}
