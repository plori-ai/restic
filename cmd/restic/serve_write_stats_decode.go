package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/restic"
)

// statNode retains fields consumed by statistics and index validation. The
// batch decoder validates every data.Node field before retaining these fields;
// projection still uses the full node decoder and its round-trip guard.
type statNode struct {
	Name     string        `json:"name"`
	Type     data.NodeType `json:"type"`
	Inode    uint64        `json:"inode"`
	DeviceID uint64        `json:"device_id"`
	Size     uint64        `json:"size"`
	Links    uint64        `json:"links"`
	Content  restic.IDs    `json:"content"`
	Subtree  *restic.ID    `json:"subtree"`
	Error    string        `json:"error"`
}

func (s *serveWriteHandler) loadStatNodes(ctx context.Context, id restic.ID) ([]statNode, error) {
	blob, err := s.LoadBlob(ctx, restic.TreeBlob, id, nil)
	if err != nil {
		return nil, err
	}
	return decodeStatNodes(blob)
}

func decodeStatNodes(blob []byte) ([]statNode, error) {
	dec := json.NewDecoder(bytes.NewReader(blob))
	if token, err := dec.Token(); err != nil || token != json.Delim('{') {
		return nil, fmt.Errorf("invalid statistics tree")
	}
	// An alias removes Node.UnmarshalJSON's repeated scan of every individual
	// node. Field types, including timestamps, xattrs and raw link bytes, remain
	// exactly data.Node's. Apply its name unquoting after the one array decode.
	type encodedNode data.Node
	// Avoid repeatedly growing the full-node array for ordinary directories.
	// This is an allocation hint, not a limit on names, fields or tree size.
	decoded := make([]encodedNode, 0, len(blob)/512)
	found := false
	for dec.More() {
		token, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("invalid statistics tree key")
		}
		if key == "nodes" && !found {
			if err := dec.Decode(&decoded); err != nil {
				return nil, err
			}
			found = true
		} else {
			var discard json.RawMessage
			if err := dec.Decode(&discard); err != nil {
				return nil, err
			}
		}
	}
	if token, err := dec.Token(); err != nil || token != json.Delim('}') || !found || decoded == nil {
		return nil, fmt.Errorf("invalid statistics tree end")
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("invalid statistics tree tail")
	}
	stats := make([]statNode, len(decoded))
	for i, n := range decoded {
		name, err := strconv.Unquote(`"` + n.Name + `"`)
		if err != nil {
			return nil, err
		}
		stats[i] = statNode{Name: name, Type: n.Type, Inode: n.Inode, DeviceID: n.DeviceID, Size: n.Size, Links: n.Links, Content: n.Content, Subtree: n.Subtree, Error: n.Error}
	}
	return stats, nil
}
