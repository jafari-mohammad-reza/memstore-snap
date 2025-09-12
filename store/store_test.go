package store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	copy(mmapSeg.mp, []byte("hello test"))
	assert.Equal(t, "h", string((mmapSeg.mp)[0]), "first byte should match written data")

	err = st.Stop()
	assert.NoError(t, err)
}

type TestRecord struct {
	ID        uint64
	Name      string
	Data      []byte
	Timestamp int64
}

func TestSnapshot(t *testing.T) {
	tmpDir := "test_snapshot"
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	opts := StoreOpt{
		SnapShot: SnapShotOpts{
			Rotate:  false,
			MinSize: 1024 * 1024 * 10,
		},
		SaveDir: tmpDir,
		Wal: WalOpts{
			Wal: false,
		},
	}

	st, err := NewStore[TestRecord](opts)
	require.NoError(t, err)
	require.NotNil(t, st)

	records := []TestRecord{
		{ID: 1, Name: "record1", Data: []byte("data1"), Timestamp: time.Now().Unix()},
		{ID: 2, Name: "record2", Data: []byte("data2"), Timestamp: time.Now().Unix()},
		{ID: 3, Name: "record3", Data: []byte("data3"), Timestamp: time.Now().Unix()},
	}

	err = st.Snapshot(records)
	assert.NoError(t, err)

	snapPath := filepath.Join(tmpDir, "snapshot.dat")
	info, err := os.Stat(snapPath)
	assert.NoError(t, err)
	assert.True(t, info.Size() > 0)

	err = st.Stop()
	assert.NoError(t, err)
}

func TestLoadSnapshot(t *testing.T) {
	tmpDir := "test_load_snapshot"
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	opts := StoreOpt{
		SnapShot: SnapShotOpts{
			Rotate:  false,
			MinSize: 1024 * 1024,
		},
		SaveDir: tmpDir,
		Wal: WalOpts{
			Wal: false,
		},
	}

	records := []TestRecord{
		{ID: 1, Name: "test1", Data: []byte("testdata1"), Timestamp: 1000},
		{ID: 2, Name: "test2", Data: []byte("testdata2"), Timestamp: 2000},
		{ID: 3, Name: "test3", Data: []byte("testdata3"), Timestamp: 3000},
	}

	st1, err := NewStore[TestRecord](opts)
	require.NoError(t, err)

	err = st1.Snapshot(records)
	require.NoError(t, err)

	err = st1.Stop()
	require.NoError(t, err)

	st2, err := NewStore[TestRecord](opts)
	require.NoError(t, err)
	defer st2.Stop()
	var loaded []TestRecord
	err = st2.LoadSnapshot(&loaded)
	assert.NoError(t, err)
	assert.Len(t, loaded, len(records))

	for i, record := range loaded {
		assert.Equal(t, records[i].ID, record.ID)
		assert.Equal(t, records[i].Name, record.Name)
		assert.Equal(t, records[i].Data, record.Data)
		assert.Equal(t, records[i].Timestamp, record.Timestamp)
	}
}

func TestSnapshotRotation(t *testing.T) {
	tmpDir := "test_rotation"
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	opts := StoreOpt{
		SnapShot: SnapShotOpts{
			Rotate:  true,
			MinSize: 1024 * 1024,
		},
		SaveDir: tmpDir,
		Wal: WalOpts{
			Wal: false,
		},
	}

	st, err := NewStore[TestRecord](opts)
	require.NoError(t, err)
	defer st.Stop()

	for i := 0; i < 3; i++ {
		records := []TestRecord{
			{ID: uint64(i), Name: fmt.Sprintf("record%d", i), Data: []byte(fmt.Sprintf("data%d", i))},
		}
		err = st.Snapshot(records)
		require.NoError(t, err)
	}

	files, err := os.ReadDir(tmpDir)
	assert.NoError(t, err)
	assert.True(t, len(files) >= 2)
}

func TestConcurrentSnapshot(t *testing.T) {
	tmpDir := "test_concurrent"
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	opts := StoreOpt{
		SnapShot: SnapShotOpts{
			Rotate:  false,
			MinSize: 1024 * 1024 * 10,
		},
		SaveDir: tmpDir,
		Wal: WalOpts{
			Wal: false,
		},
	}

	st, err := NewStore[TestRecord](opts)
	require.NoError(t, err)
	defer st.Stop()

	done := make(chan bool, 2)
	errors := make(chan error, 2)

	go func() {
		records := make([]TestRecord, 100)
		for i := range records {
			records[i] = TestRecord{ID: uint64(i), Name: fmt.Sprintf("goroutine1-%d", i)}
		}
		if err := st.Snapshot(records); err != nil {
			errors <- err
		}
		done <- true
	}()

	go func() {
		records := make([]TestRecord, 100)
		for i := range records {
			records[i] = TestRecord{ID: uint64(i + 1000), Name: fmt.Sprintf("goroutine2-%d", i)}
		}
		if err := st.Snapshot(records); err != nil {
			errors <- err
		}
		done <- true
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case err := <-errors:
			t.Fatalf("concurrent snapshot error: %v", err)
		}
	}
}

func TestLargeSnapshot(t *testing.T) {
	tmpDir := "test_large_snapshot"
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	opts := StoreOpt{
		SnapShot: SnapShotOpts{
			Rotate:  false,
			MinSize: 1024 * 1024 * 100,
		},
		SaveDir: tmpDir,
		Wal: WalOpts{
			Wal: false,
		},
	}

	st, err := NewStore[TestRecord](opts)
	require.NoError(t, err)
	defer st.Stop()

	records := make([]TestRecord, 10000)
	for i := range records {
		records[i] = TestRecord{
			ID:        uint64(i),
			Name:      fmt.Sprintf("record-%d", i),
			Data:      make([]byte, 1024),
			Timestamp: time.Now().Unix(),
		}
		for j := range records[i].Data {
			records[i].Data[j] = byte(i % 256)
		}
	}

	err = st.Snapshot(records)
	assert.NoError(t, err)
	var loaded []TestRecord
	err = st.LoadSnapshot(&loaded)
	assert.NoError(t, err)
	assert.Len(t, loaded, len(records))
}

func BenchmarkSnapshot(b *testing.B) {
	tmpDir := "bench_snapshot"
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	opts := StoreOpt{
		SnapShot: SnapShotOpts{
			Rotate:  false,
			MinSize: 1024 * 1024 * 100,
		},
		SaveDir: tmpDir,
		Wal: WalOpts{
			Wal: false,
		},
	}

	st, err := NewStore[TestRecord](opts)
	require.NoError(b, err)
	defer st.Stop()

	records := make([]TestRecord, 1000)
	for i := range records {
		records[i] = TestRecord{
			ID:        uint64(i),
			Name:      fmt.Sprintf("record-%d", i),
			Data:      make([]byte, 100),
			Timestamp: time.Now().Unix(),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := st.Snapshot(records)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLoadSnapshot(b *testing.B) {
	tmpDir := "bench_load_snapshot"
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	opts := StoreOpt{
		SnapShot: SnapShotOpts{
			Rotate:  false,
			MinSize: 1024 * 1024 * 100,
		},
		SaveDir: tmpDir,
		Wal: WalOpts{
			Wal: false,
		},
	}

	st, err := NewStore[TestRecord](opts)
	require.NoError(b, err)

	records := make([]TestRecord, 10000)
	for i := range records {
		records[i] = TestRecord{
			ID:        uint64(i),
			Name:      fmt.Sprintf("record-%d", i),
			Data:      make([]byte, 100),
			Timestamp: time.Now().Unix(),
		}
	}

	err = st.Snapshot(records)
	require.NoError(b, err)
	st.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st, err := NewStore[TestRecord](opts)
		if err != nil {
			b.Fatal(err)
		}
		var loaded []TestRecord
		err = st.LoadSnapshot(&loaded)
		if err != nil {
			b.Fatal(err)
		}

		st.Stop()
	}
}

func BenchmarkSnapshot1Million(b *testing.B) {
	tmpDir := "bench_1m_snapshot"
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	opts := StoreOpt{
		SnapShot: SnapShotOpts{
			Rotate:  false,
			MinSize: 1024 * 1024 * 1000,
		},
		SaveDir: tmpDir,
		Wal: WalOpts{
			Wal: false,
		},
	}

	st, err := NewStore[TestRecord](opts)
	require.NoError(b, err)
	defer st.Stop()

	records := make([]TestRecord, 1000000)
	for i := range records {
		records[i] = TestRecord{
			ID:        uint64(i),
			Name:      fmt.Sprintf("rec%d", i),
			Data:      []byte{byte(i % 256)},
			Timestamp: int64(i),
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	start := time.Now()
	err = st.Snapshot(records)
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("Snapshot 1M records took: %v", time.Since(start))
}

func BenchmarkLoad1Million(b *testing.B) {
	tmpDir := "bench_1m_load"
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	opts := StoreOpt{
		SnapShot: SnapShotOpts{
			Rotate:  false,
			MinSize: 1024 * 1024 * 1000,
		},
		SaveDir: tmpDir,
		Wal: WalOpts{
			Wal: false,
		},
	}

	st, err := NewStore[TestRecord](opts)
	require.NoError(b, err)

	records := make([]TestRecord, 1000000)
	for i := range records {
		records[i] = TestRecord{
			ID:        uint64(i),
			Name:      fmt.Sprintf("rec%d", i),
			Data:      []byte{byte(i % 256)},
			Timestamp: int64(i),
		}
	}

	err = st.Snapshot(records)
	require.NoError(b, err)
	st.Stop()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		st, err := NewStore[TestRecord](opts)
		if err != nil {
			b.Fatal(err)
		}

		start := time.Now()
		var loaded []TestRecord
		err = st.LoadSnapshot(&loaded)
		if err != nil {
			b.Fatal(err)
		}

		if i == 0 {
			b.Logf("Load 1M records took: %v, loaded: %d", time.Since(start), len(loaded))
		}

		st.Stop()
	}
}

func BenchmarkParallelSnapshot(b *testing.B) {
	tmpDir := "bench_parallel_snapshot"
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	opts := StoreOpt{
		SnapShot: SnapShotOpts{
			Rotate:  false,
			MinSize: 1024 * 1024 * 100,
		},
		SaveDir: tmpDir,
		Wal: WalOpts{
			Wal: false,
		},
	}

	st, err := NewStore[TestRecord](opts)
	require.NoError(b, err)
	defer st.Stop()

	records := make([]TestRecord, 10000)
	for i := range records {
		records[i] = TestRecord{
			ID:   uint64(i),
			Name: fmt.Sprintf("record-%d", i),
			Data: make([]byte, 100),
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := st.Snapshot(records)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
