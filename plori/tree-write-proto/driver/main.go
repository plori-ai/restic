// Command driver sends chained serve-write requests over a Unix socket, logs
// the edits, applies the same edits to a directory with the helper's POSIX
// steps (workspacehelper/mutation.go) and compares snapshots node by node.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"time"
)

type edit struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	To    string `json:"to,omitempty"`
	Trash string `json:"trash,omitempty"`
	Data  []byte `json:"data,omitempty"`
	Mode  uint32 `json:"mode,omitempty"`
}

type state struct {
	Head   string     `json:"head"`
	Single []string   `json:"single"`
	Pairs  [][]string `json:"pairs"`
	Dirs   []string   `json:"dirs"`
	Seq    int        `json:"seq"`
}

type receipt struct {
	Snapshot     string `json:"snapshot"`
	Tree         string `json:"tree"`
	Entries      uint64 `json:"entries"`
	LogicalBytes uint64 `json:"logical_bytes"`
}

type response struct {
	Empty      bool               `json:"empty"`
	Head       *receipt           `json:"head"`
	Public     *receipt           `json:"public"`
	Incomplete []string           `json:"incomplete_link_groups"`
	Timings    map[string]float64 `json:"timings_ms"`
}

func client(socket string) *http.Client {
	return &http.Client{Timeout: 10 * time.Minute, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}}
}

func post(c *http.Client, uri string, body any) (response, time.Duration, error) {
	b, _ := json.Marshal(body)
	start := time.Now()
	resp, err := c.Post("http://sw"+uri, "application/json", bytes.NewReader(b))
	if err != nil {
		return response{}, 0, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	elapsed := time.Since(start)
	if err != nil {
		return response{}, elapsed, err
	}
	if resp.StatusCode != http.StatusOK {
		return response{}, elapsed, fmt.Errorf("%s: %d %s", uri, resp.StatusCode, raw)
	}
	var r response
	return r, elapsed, json.Unmarshal(raw, &r)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "driver:", err)
		os.Exit(1)
	}
}

func readJSON(p string, v any) {
	b, err := os.ReadFile(p)
	must(err)
	must(json.Unmarshal(b, v))
}

func writeJSON(p string, v any) {
	b, err := json.MarshalIndent(v, "", " ")
	must(err)
	must(os.WriteFile(p, b, 0o644))
}

func percentile(v []float64, p float64) float64 {
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	i := int(p*float64(len(s)-1) + 0.5)
	return s[i]
}

func main() {
	if len(os.Args) < 2 {
		must(errors.New("usage: driver init|prepare|run|apply|compare"))
	}
	fsFlags := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	socket := fsFlags.String("socket", "", "serve-write socket")
	stateFile := fsFlags.String("state", "", "state JSON")
	manifest := fsFlags.String("manifest", "", "generator manifest")
	base := fsFlags.String("base", "", "base snapshot")
	scenario := fsFlags.String("scenario", "", "overwrite|rename|pair|trash")
	n := fsFlags.Int("n", 10, "edits")
	logFile := fsFlags.String("log", "", "edit log (JSON lines)")
	out := fsFlags.String("out", "", "result JSON")
	dir := fsFlags.String("dir", "", "tree directory")
	from := fsFlags.Int("from", 0, "first log line to apply")
	to := fsFlags.Int("to", -1, "line after the last to apply")
	a := fsFlags.String("a", "", "snapshot A")
	b := fsFlags.String("b", "", "snapshot B")
	owner := fsFlags.Int("owner", os.Getuid(), "owner uid and gid of created nodes")
	must(fsFlags.Parse(os.Args[2:]))
	switch os.Args[1] {
	case "init":
		var m struct {
			Dirs   []string   `json:"dirs"`
			Single []string   `json:"single"`
			Pairs  [][]string `json:"pairs"`
		}
		readJSON(*manifest, &m)
		writeJSON(*stateFile, state{Head: *base, Single: m.Single, Pairs: m.Pairs, Dirs: m.Dirs})
	case "prepare":
		r, d, err := post(client(*socket), "/prepare-write", map[string]string{"snapshot": *base})
		must(err)
		fmt.Printf("{\"prepare_ms\": %.1f, \"server_ms\": %v}\n", float64(d.Microseconds())/1000, r.Timings)
	case "run":
		run(client(*socket), *stateFile, *scenario, *n, *logFile, *out, uint32(*owner))
	case "apply":
		apply(*dir, *logFile, *from, *to, *owner)
	case "compare":
		os.Exit(compare(client(*socket), *a, *b))
	case "mergebig":
		mergeBig(client(*socket), *base, *n, *out, uint32(*owner))
	case "merge":
		mergeRun(client(*socket), *manifest, *base, *a, *b, *n, *out, uint32(*owner))
	}
}

func run(c *http.Client, stateFile, scenario string, n int, logFile, out string, owner uint32) {
	var st state
	readJSON(stateFile, &st)
	rng := rand.New(rand.NewSource(int64(len(st.Single)*7919 + st.Seq)))
	lf, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	must(err)
	defer lf.Close()
	var lat []float64
	var timings []map[string]float64
	var last response
	for i := 0; i < n; i++ {
		st.Seq++
		var e edit
		switch scenario {
		case "overwrite":
			p := st.Single[rng.Intn(len(st.Single))]
			e = edit{Op: "write", Path: p, Data: randBytes(rng, 1024)}
		case "rename":
			k := rng.Intn(len(st.Single))
			p := st.Single[k]
			d := st.Dirs[rng.Intn(len(st.Dirs))]
			to := fmt.Sprintf("%s/renamed-%05d.txt", d, st.Seq)
			e = edit{Op: "rename", Path: p, To: to}
			st.Single[k] = to
		case "pair":
			pair := st.Pairs[0]
			e = edit{Op: "write", Path: pair[i%2], Data: randBytes(rng, 700)}
		case "trash":
			pair := st.Pairs[1]
			if i == 0 {
				e = edit{Op: "delete", Path: pair[1], Trash: fmt.Sprintf("op-%05d", st.Seq)}
			} else {
				e = edit{Op: "write", Path: pair[0], Data: randBytes(rng, 900)}
			}
		default:
			must(fmt.Errorf("scenario %q", scenario))
		}
		req := map[string]any{"base": st.Head, "op_id": fmt.Sprintf("00000000-0000-4000-8000-%012d", st.Seq), "copy_id": "proto-copy",
			"owner": []uint32{owner, owner}, "edits": []edit{e}}
		r, d, err := post(c, "/edit", req)
		must(err)
		line, _ := json.Marshal(e)
		_, err = lf.Write(append(line, '\n'))
		must(err)
		lat = append(lat, float64(d.Microseconds())/1000)
		timings = append(timings, r.Timings)
		st.Head = r.Head.Snapshot
		last = r
	}
	writeJSON(stateFile, st)
	res := map[string]any{"scenario": scenario, "n": n, "latency_ms": lat, "p50_ms": percentile(lat, 0.5), "p95_ms": percentile(lat, 0.95),
		"max_ms": percentile(lat, 1), "server_timings_ms": timings, "last": last}
	writeJSON(out, res)
	fmt.Printf("{\"scenario\":%q,\"n\":%d,\"p50_ms\":%.1f,\"p95_ms\":%.1f,\"max_ms\":%.1f,\"head\":%q,\"public\":%v,\"incomplete\":%d}\n",
		scenario, n, percentile(lat, 0.5), percentile(lat, 0.95), percentile(lat, 1), st.Head, last.Public != nil, len(last.Incomplete))
}

func randBytes(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	rng.Read(b)
	return b
}

// apply performs logged edits on dir with the helper's steps: a file with
// several names is rewritten in place, any other write goes to a temporary
// file renamed over the path, missing parents get mode 0755, delete moves to
// .plori-trash/<handle> (mode 0700), rename keeps the inode.
func apply(dir, logFile string, from, to, owner int) {
	f, err := os.Open(logFile)
	must(err)
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	i := 0
	for sc.Scan() {
		if i < from || (to >= 0 && i >= to) {
			i++
			continue
		}
		i++
		var e edit
		must(json.Unmarshal(sc.Bytes(), &e))
		p := filepath.Join(dir, e.Path)
		switch e.Op {
		case "write":
			st, err := os.Lstat(p)
			if err == nil && st.Sys().(*syscall.Stat_t).Nlink > 1 {
				h, err := os.OpenFile(p, os.O_WRONLY|syscall.O_NOFOLLOW, 0)
				must(err)
				must(h.Truncate(0))
				_, err = h.Write(e.Data)
				must(err)
				must(h.Close())
				continue
			}
			mode := os.FileMode(0o644)
			if err == nil {
				mode = st.Mode().Perm()
			}
			ensureParents(dir, e.Path)
			tmp := filepath.Join(filepath.Dir(p), fmt.Sprintf(".plori-op-%d", i))
			h, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			must(err)
			_, err = h.Write(e.Data)
			must(err)
			must(h.Chmod(mode))
			must(h.Close())
			must(os.Rename(tmp, p))
		case "rename":
			ensureParents(dir, e.To)
			must(os.Rename(p, filepath.Join(dir, e.To)))
		case "delete":
			trash := filepath.Join(dir, ".plori-trash")
			if _, err := os.Lstat(trash); errors.Is(err, fs.ErrNotExist) {
				must(os.Mkdir(trash, 0o700))
				must(os.Chmod(trash, 0o700))
			}
			must(os.Rename(p, filepath.Join(trash, e.Trash)))
		default:
			must(fmt.Errorf("op %q", e.Op))
		}
	}
	must(sc.Err())
	_ = owner
}

func ensureParents(dir, p string) {
	parent := path.Dir(p)
	if parent == "." {
		return
	}
	if _, err := os.Lstat(filepath.Join(dir, parent)); err == nil {
		return
	}
	ensureParents(dir, parent)
	must(os.Mkdir(filepath.Join(dir, parent), 0o755))
	must(os.Chmod(filepath.Join(dir, parent), 0o755))
}

type node struct {
	Name       string            `json:"name"`
	Type       string            `json:"type"`
	Mode       uint64            `json:"mode"`
	Size       uint64            `json:"size"`
	Links      uint64            `json:"links"`
	UID        uint32            `json:"uid"`
	GID        uint32            `json:"gid"`
	Inode      uint64            `json:"inode"`
	DeviceID   uint64            `json:"device_id"`
	LinkTarget string            `json:"linktarget"`
	Content    []string          `json:"content"`
	Xattrs     []json.RawMessage `json:"extended_attributes"`
	Generic    json.RawMessage   `json:"generic_attributes"`
}

func walk(c *http.Client, id string) map[string]node {
	resp, err := c.Get("http://sw/walk?snapshot=" + id)
	must(err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		must(fmt.Errorf("walk %s: %d", id, resp.StatusCode))
	}
	out := map[string]node{}
	dec := json.NewDecoder(resp.Body)
	for {
		var rec struct {
			Path string `json:"path"`
			Node node   `json:"node"`
		}
		if err := dec.Decode(&rec); err == io.EOF {
			break
		} else {
			must(err)
		}
		out[rec.Path] = rec.Node
	}
	if resp.Trailer.Get("X-Restic-Error") != "" {
		must(errors.New("walk failed"))
	}
	return out
}

func groups(nodes map[string]node) []string {
	g := map[[2]uint64][]string{}
	for p, n := range nodes {
		if n.Type == "file" && n.Links > 1 {
			k := [2]uint64{n.DeviceID, n.Inode}
			g[k] = append(g[k], p)
		}
	}
	var out []string
	for _, names := range g {
		sort.Strings(names)
		out = append(out, strings.Join(names, ","))
	}
	sort.Strings(out)
	return out
}

// compare reports differences in names, type, mode, size, link count,
// owner, symlink target, content blob IDs, xattrs and hard-link groups.
// Inode numbers, devices and times are not compared.
func compare(c *http.Client, a, b string) int {
	na, nb := walk(c, a), walk(c, b)
	diffs := 0
	report := func(format string, args ...any) {
		diffs++
		if diffs <= 20 {
			fmt.Printf(format+"\n", args...)
		}
	}
	for p, x := range na {
		y, ok := nb[p]
		if !ok {
			report("only in A: %s", p)
			continue
		}
		x.Inode, y.Inode, x.DeviceID, y.DeviceID = 0, 0, 0, 0
		if !reflect.DeepEqual(x, y) {
			ja, _ := json.Marshal(x)
			jb, _ := json.Marshal(y)
			report("differs: %s\n A %s\n B %s", p, ja, jb)
		}
	}
	for p := range nb {
		if _, ok := na[p]; !ok {
			report("only in B: %s", p)
		}
	}
	ga, gb := groups(na), groups(nb)
	if !reflect.DeepEqual(ga, gb) {
		report("hard-link groups differ: %d vs %d", len(ga), len(gb))
	}
	inodes := map[uint64]int{}
	for _, x := range na {
		inodes[x.Inode]++
	}
	collisions := 0
	for _, x := range na {
		if inodes[x.Inode] > 1 && (x.Type != "file" || x.Links != uint64(inodes[x.Inode])) {
			collisions++
		}
	}
	fmt.Printf("{\"nodes_a\":%d,\"nodes_b\":%d,\"link_groups\":%d,\"differences\":%d,\"inode_collisions_a\":%d}\n", len(na), len(nb), len(ga), diffs, collisions)
	if diffs != 0 || collisions != 0 {
		return 1
	}
	return 0
}

// mergeRun posts the merge result in file (entries and generated blobs) n
// times against current with sources a and b and reports the latencies.
func mergeRun(c *http.Client, file, current, a, b string, n int, out string, owner uint32) {
	var m struct {
		Entries json.RawMessage   `json:"entries"`
		Blobs   map[string][]byte `json:"blobs"`
	}
	readJSON(file, &m)
	var lat []float64
	var last response
	var timings []map[string]float64
	for i := 0; i < n; i++ {
		req := map[string]any{"base": current, "sources": []string{a, b}, "entries": m.Entries, "blobs": m.Blobs,
			"op_id": fmt.Sprintf("00000000-0000-4000-9000-%012d", i), "copy_id": "integration", "owner": []uint32{owner, owner}}
		r, d, err := post(c, "/merge-write", req)
		must(err)
		lat = append(lat, float64(d.Microseconds())/1000)
		timings = append(timings, r.Timings)
		last = r
	}
	writeJSON(out, map[string]any{"latency_ms": lat, "p50_ms": percentile(lat, 0.5), "p95_ms": percentile(lat, 0.95), "server_timings_ms": timings, "last": last})
	fmt.Printf("{\"n\":%d,\"first_ms\":%.1f,\"p50_ms\":%.1f,\"p95_ms\":%.1f,\"head\":%q,\"public\":%v}\n", n, lat[0], percentile(lat, 0.5), percentile(lat, 0.95), last.Head.Snapshot, last.Public != nil)
}

// mergeBig turns the snapshot base (a public twin) into a merge manifest,
// removes one file, changes one mode and adds a generated file, and posts it
// n times as a merge result against base.
func mergeBig(c *http.Client, base string, n int, out string, owner uint32) {
	nodes := walk(c, base)
	groups := map[[2]uint64][]string{}
	for p, x := range nodes {
		if x.Type == "file" && x.Links > 1 {
			k := [2]uint64{x.DeviceID, x.Inode}
			groups[k] = append(groups[k], strings.TrimPrefix(p, "/"))
		}
	}
	labels := map[[2]uint64]string{}
	for k, names := range groups {
		sort.Strings(names)
		b, _ := json.Marshal(names)
		h := sha256.Sum256(b)
		labels[k] = hex.EncodeToString(h[:])
	}
	type entry struct {
		Path      string `json:"path"`
		Kind      string `json:"kind"`
		Mode      uint32 `json:"mode"`
		Size      int64  `json:"size"`
		Digest    string `json:"digest,omitempty"`
		Target    string `json:"target,omitempty"`
		LinkGroup string `json:"link_group,omitempty"`
	}
	var entries []entry
	var paths []string
	for p := range nodes {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	removed, chmodded := "", ""
	for _, p := range paths {
		x := nodes[p]
		e := entry{Path: strings.TrimPrefix(p, "/"), Kind: x.Type, Mode: uint32(x.Mode & 0o777)}
		switch x.Type {
		case "file":
			h := sha256.New()
			for _, id := range x.Content {
				b, _ := hex.DecodeString(id)
				h.Write(b)
			}
			e.Size, e.Digest = int64(x.Size), "restic:"+hex.EncodeToString(h.Sum(nil))
			if x.Links > 1 {
				e.LinkGroup = labels[[2]uint64{x.DeviceID, x.Inode}]
			} else if removed == "" && strings.Contains(p, "/f") {
				removed = p
				continue
			} else if chmodded == "" && strings.Contains(p, "/f") && x.Mode&0o777 != 0o600 {
				chmodded = p
				e.Mode = 0o600
			}
		case "symlink":
			e.Target, e.Size = x.LinkTarget, int64(len(x.LinkTarget))
		}
		entries = append(entries, e)
	}
	gen := []byte("<<<<<<< current\nA\n=======\nB\n>>>>>>> incoming\n")
	sum := sha256.Sum256(gen)
	digest := hex.EncodeToString(sum[:])
	entries = append(entries, entry{Path: "merge-new", Kind: "dir", Mode: 0o755}, entry{Path: "merge-new/generated.txt", Kind: "file", Mode: 0o644, Size: int64(len(gen)), Digest: digest})
	var lat []float64
	var timings []map[string]float64
	var last response
	for i := 0; i < n; i++ {
		req := map[string]any{"base": base, "entries": entries, "blobs": map[string][]byte{digest: gen},
			"op_id": fmt.Sprintf("00000000-0000-4000-a000-%012d", i), "copy_id": "integration", "owner": []uint32{owner, owner}}
		r, d, err := post(c, "/merge-write", req)
		must(err)
		lat = append(lat, float64(d.Microseconds())/1000)
		timings = append(timings, r.Timings)
		last = r
	}
	writeJSON(out, map[string]any{"entries": len(entries), "removed": removed, "chmodded": chmodded, "latency_ms": lat, "server_timings_ms": timings, "last": last})
	fmt.Printf("{\"entries\":%d,\"n\":%d,\"first_ms\":%.1f,\"p50_ms\":%.1f,\"max_ms\":%.1f,\"head\":%q,\"public\":%v}\n", len(entries), n, lat[0], percentile(lat, 0.5), percentile(lat, 1), last.Head.Snapshot, last.Public != nil)
}
