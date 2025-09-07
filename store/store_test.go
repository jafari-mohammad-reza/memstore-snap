package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateStorage(t *testing.T) {
	tmpDir := "test_store_snaps"

	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	opts := StoreOpt{
		SnapShot: SnapShotOpts{
			Rotate:  false,
			MinSize: 1024 * 1024,
		},
		SaveDir: tmpDir,
		Wal: WalOpts{
			Wal: true,
		},
	}

	st, err := NewStore[[]byte](opts)
	assert.NoError(t, err)
	assert.NotNil(t, st)

	info, err := os.Stat(tmpDir)
	assert.NoError(t, err)
	assert.True(t, info.IsDir(), "store directory should exist")

	walPath := filepath.Join(tmpDir, "wal.log")
	info, err = os.Stat(walPath)
	assert.NoError(t, err)
	assert.False(t, info.IsDir(), "WAL file should exist")

	snapPath := filepath.Join(tmpDir, "snapshot.dat")
	info, err = os.Stat(snapPath)
	assert.NoError(t, err)
	assert.False(t, info.IsDir(), "Snapshot file should exist")

	mmapSeg := st.segFiles["snapshot.dat"]
	copy(*mmapSeg, []byte("hello test"))
	assert.Equal(t, "h", string((*mmapSeg)[0]), "first byte should match written data")

	err = st.Stop()
	assert.NoError(t, err)
}
