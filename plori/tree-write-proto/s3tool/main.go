// Command s3tool lists (sizes) or deletes every object under a prefix of the
// staging MinIO bucket. Credentials come from AWS_ACCESS_KEY_ID and
// AWS_SECRET_ACCESS_KEY; they are never printed.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	if len(os.Args) != 5 || (os.Args[1] != "size" && os.Args[1] != "delete") {
		fmt.Fprintln(os.Stderr, "usage: s3tool size|delete ENDPOINT BUCKET PREFIX")
		os.Exit(2)
	}
	op, endpoint, bucket, prefix := os.Args[1], os.Args[2], os.Args[3], os.Args[4]
	if !strings.HasPrefix(prefix, "proto-tree-write/") || (op == "delete" && len(prefix) < len("proto-tree-write/")+8) {
		fmt.Fprintln(os.Stderr, "refusing prefix outside proto-tree-write/<uuid>")
		os.Exit(2)
	}
	c, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewEnvAWS(), Secure: false})
	if err != nil {
		fmt.Fprintln(os.Stderr, "client:", err)
		os.Exit(1)
	}
	ctx := context.Background()
	var count, bytes int64
	objects := make(chan minio.ObjectInfo)
	go func() {
		defer close(objects)
		for o := range c.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			if o.Err != nil {
				fmt.Fprintln(os.Stderr, "list:", o.Err)
				os.Exit(1)
			}
			count++
			bytes += o.Size
			if op == "delete" {
				objects <- o
			}
		}
	}()
	if op == "delete" {
		for e := range c.RemoveObjects(ctx, bucket, objects, minio.RemoveObjectsOptions{}) {
			fmt.Fprintln(os.Stderr, "remove:", e.ObjectName, e.Err)
			os.Exit(1)
		}
	} else {
		for range objects {
		}
	}
	fmt.Printf("{\"op\":%q,\"objects\":%d,\"bytes\":%d}\n", op, count, bytes)
}
