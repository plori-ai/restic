package repository

import (
	"context"
	"testing"

	"github.com/restic/restic/internal/crypto"
	rtest "github.com/restic/restic/internal/test"
)

func TestUseMinimalKDFParameters(t *testing.T) {
	repo := TestRepository(t)
	old := params
	defer func() { params = old }()

	UseMinimalKDFParameters()
	key, err := AddKey(context.TODO(), repo, "high-entropy-secret", "user", "host", repo.Key())
	rtest.OK(t, err)

	stored, err := LoadKey(context.TODO(), repo, key.ID())
	rtest.OK(t, err)
	got := crypto.Params{N: stored.N, R: stored.R, P: stored.P}
	rtest.Equals(t, MinimalKDFParams, got)

	opened, err := OpenKey(context.TODO(), repo, key.ID(), "high-entropy-secret")
	rtest.OK(t, err)
	rtest.Equals(t, repo.Key().MACKey, opened.master.MACKey)
	rtest.Equals(t, repo.Key().EncryptionKey, opened.master.EncryptionKey)

	_, err = OpenKey(context.TODO(), repo, key.ID(), "wrong")
	rtest.Assert(t, err != nil, "wrong password opened a minimal-KDF key")
}
