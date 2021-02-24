package cache

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/google/go-containerregistry/pkg/v1/validate"
)

// TestCache tests that the cache is populated when LayerByDigest is called.
func TestCache(t *testing.T) {
	numLayers := 5
	img, err := random.Image(10, int64(numLayers))
	if err != nil {
		t.Fatalf("random.Image: %v", err)
	}
	m := &memcache{map[v1.Hash]v1.Layer{}, map[v1.Hash]v1.Layer{}}
	img = Image(img, m)

	// Cache is empty.
	if len(m.byDiffID) != 0 || len(m.byDigest) != 0 {
		t.Errorf("Before consuming, cache is non-empty: %+v, %+v", m.byDiffID, m.byDigest)
	}

	// Consume each layer, cache gets populated.
	if _, err := img.Layers(); err != nil {
		t.Fatalf("Layers: %v", err)
	}
	if got, want := len(m.byDiffID), numLayers; got != want {
		t.Errorf("DiffID cache has %d entries, want %d", got, want)
	}
	if got, want := len(m.byDigest), numLayers; got != want {
		t.Errorf("Digest cache has %d entries, want %d", got, want)
	}
}

// breakableImageWrapper calls the upstream Image when broken is false
// when broken is set, it metadata calls, not layer data
type breakableImageWrapper struct {
	v1.Image
	broken bool
}

// brokenLayer is a layer with only metadata available,
// Compressed()/Uncompressed() fails
type brokenLayer struct {
	v1.Layer
}

func (b *brokenLayer) Digest() (v1.Hash, error)            { return b.Layer.Digest() }
func (b *brokenLayer) DiffID() (v1.Hash, error)            { return b.Layer.DiffID() }
func (b *brokenLayer) Size() (int64, error)                { return b.Layer.Size() }
func (b *brokenLayer) MediaType() (types.MediaType, error) { return b.Layer.MediaType() }

func (b *brokenLayer) Compressed() (io.ReadCloser, error) {
	panic("should not be called when caching is enabled")
}

func (b *brokenLayer) Uncompressed() (io.ReadCloser, error) {
	panic("should not be called when caching is enabled")
}

func (i *breakableImageWrapper) Layers() ([]v1.Layer, error) {
	if !i.broken {
		return i.Image.Layers()
	}
	ls, err := i.Image.Layers()
	if err != nil {
		return nil, err
	}

	var out []v1.Layer
	for _, l := range ls {
		out = append(out, &brokenLayer{l})
	}
	return out, nil
}

func (i *breakableImageWrapper) LayerByDigest(h v1.Hash) (v1.Layer, error) {
	if !i.broken {
		return i.Image.LayerByDigest(h)
	}

	panic("LayerByDigest should not be called when caching is enabled")
}

func (i *breakableImageWrapper) LayerByDiffID(h v1.Hash) (v1.Layer, error) {
	if !i.broken {
		return i.Image.LayerByDiffID(h)
	}

	panic("LayerByDiffID should not be called when caching is enabled")
}

func TestImageMemory(t *testing.T) {
	original, err := random.Image(1024, 5)
	if err != nil {
		t.Fatalf("random.Image: %v", err)
	}
	breakable := &breakableImageWrapper{Image: original}
	m := &memcache{map[v1.Hash]v1.Layer{}, map[v1.Hash]v1.Layer{}}
	img := Image(breakable, m)

	// Validate twice to hit the cache.
	if err := validate.Image(img); err != nil {
		t.Errorf("Validate: %v", err)
	}

	breakable.broken = true
	if err := validate.Image(img); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

func TestImageFS(t *testing.T) {
	dir, err := ioutil.TempDir("", "image-cache")
	if err != nil {
		t.Fatalf("TempDir: %v", err)
	}
	defer os.RemoveAll(dir)

	original, err := random.Image(1024, 5)
	if err != nil {
		t.Fatalf("random.Image: %v", err)
	}
	breakable := &breakableImageWrapper{Image: original}
	m := &fscache{path: dir}
	img := Image(breakable, m)

	// Validate twice to hit the cache.
	if err := validate.Image(img); err != nil {
		t.Errorf("Validate: %v", err)
	}

	breakable.broken = true
	if err := validate.Image(img); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

// TestCacheShortCircuitDiffID tests that if a layer is found in the cache,
// LayerByDigest is not called in the underlying Image implementation.
func TestCacheShortCircuitDiffID(t *testing.T) {
	l := &fakeLayer{}
	m := &memcache{
		byDiffID: map[v1.Hash]v1.Layer{
			fakeHash: l,
		},
		byDigest: map[v1.Hash]v1.Layer{},
	}
	img := Image(&fakeImage{}, m)

	for i := 0; i < 10; i++ {
		if _, err := img.LayerByDiffID(fakeHash); err != nil {
			t.Errorf("LayerByDiffID[%d]: %v", i, err)
		}
	}
}

// TestCacheShortCircuitDigest tests that if a layer is found in the cache,
// LayerByDigest is not called in the underlying Image implementation.
func TestCacheShortCircuitDigest(t *testing.T) {
	l := &fakeLayer{}
	m := &memcache{
		byDiffID: map[v1.Hash]v1.Layer{},
		byDigest: map[v1.Hash]v1.Layer{
			fakeHash: l,
		},
	}
	img := Image(&fakeImage{}, m)

	for i := 0; i < 10; i++ {
		if _, err := img.LayerByDigest(fakeHash); err != nil {
			t.Errorf("LayerByDigest[%d]: %v", i, err)
		}
	}
}

var fakeHash = v1.Hash{Algorithm: "fake", Hex: "data"}

type fakeLayer struct{ v1.Layer }
type fakeImage struct{ v1.Image }

func (f *fakeImage) LayerByDigest(v1.Hash) (v1.Layer, error) {
	return nil, errors.New("LayerByDigest was called")
}

// memcache is an in-memory Cache implementation.
//
// It doesn't intend to actually write layer data, it just keeps a reference
// to the original Layer.
//
// It only assumes/considers compressed layers, and so only writes layers by
// digest.
type memcache struct {
	byDiffID map[v1.Hash]v1.Layer
	byDigest map[v1.Hash]v1.Layer
}

func (m *memcache) Put(l v1.Layer) (v1.Layer, error) {
	digest, err := l.Digest()
	if err != nil {
		return nil, err
	}
	m.byDigest[digest] = l

	diffID, err := l.DiffID()
	if err != nil {
		return nil, err
	}
	m.byDiffID[diffID] = l
	return l, nil
}

func (m *memcache) Get(h v1.Hash) (v1.Layer, error) {
	if l, found := m.byDiffID[h]; found {
		return l, nil
	}
	if l, found := m.byDigest[h]; found {
		return l, nil
	}
	return nil, ErrNotFound
}

func (m *memcache) Delete(h v1.Hash) error {
	l, err := m.Get(h)
	if err != nil {
		return err // or nil?
	}

	if diffID, err := l.DiffID(); err == nil {
		delete(m.byDiffID, diffID)
	}

	if digest, err := l.Digest(); err == nil {
		delete(m.byDigest, digest)
	}

	return nil
}
