package main

import (
	"context"
	"strings"
	"testing"

	"github.com/restic/restic/internal/repository"
	rtest "github.com/restic/restic/internal/test"
)

func TestServeReadSkeleton(t *testing.T) {
	writer, be := repository.TestRepositoryWithBackend(t, nil, 0, repository.Options{})
	id, _ := serveReadFixture(t, writer, "abcdefghijklmno")
	reader := repository.TestOpenBackend(t, be)
	rtest.OK(t, reader.LoadIndex(context.Background(), nil))
	h := newServeReadHandler(reader)
	w := serveReadRequest(h, "GET", "/skeleton?snapshot="+id.String(), "")
	rtest.Equals(t, 200, w.Code)
	rtest.Equals(t, "", w.Result().Trailer.Get("X-Restic-Error"))
	const mt = "1234567890123456789"
	want := strings.Join([]string{
		`{"p":"/dir","t":"d","m":488,"u":0,"g":0,"s":0,"mt":0,"at":0}`,
		`{"p":"/dir/file","t":"f","m":416,"u":0,"g":0,"s":15,"mt":` + mt + `,"at":0,"n":2,"i":123,"v":456}`,
		`{"p":"/dir/hardlink","t":"f","m":416,"u":0,"g":0,"s":15,"mt":` + mt + `,"at":0,"n":2,"i":123,"v":456}`,
		`{"p":"/dir/symlink","t":"l","m":511,"u":0,"g":0,"s":0,"mt":0,"at":0,"l":"file"}`,
	}, "\n") + "\n"
	rtest.Equals(t, want, w.Body.String())
}
