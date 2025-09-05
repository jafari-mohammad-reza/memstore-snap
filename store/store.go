package store

import (
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
)

// there should be options as rotation, multiple snapshot files
type Store[T any] struct {
	opt      StoreOpt
	walFile  *os.File
	segFiles map[string]*mmap.MMap // map of rotation period to file
	walChan  chan WalRec
	walMu    sync.Mutex
}

func NewStore[T any](opts StoreOpt) (*Store[T], error) {
	if err := prepDir(opts); err != nil {
		return nil, fmt.Errorf("failed to create dir: %s", err.Error())
	}
	segs, err := prepSegs(opts)
	if err != nil {
		return nil, err
	}
	if opts.Wal.ChanSize == 0 {
		opts.Wal.ChanSize = 1000
	}
	wal, err := prepWal(opts)
	if err != nil {
		return nil, err
	}
	return &Store[T]{
		walMu:    sync.Mutex{},
		walFile:  wal,
		walChan:  make(chan WalRec, opts.Wal.WalSize),
		segFiles: segs,
		opt:      opts,
	}, nil
}
func (s *Store[T]) Stop() error {
	close(s.walChan)
	if err := s.walFile.Close(); err != nil {
		return err
	}
	for _, f := range s.segFiles {
		if err := f.Unmap(); err != nil {
			return err
		}
	}
	return nil
}
func prepDir(opts StoreOpt) error {
	dir := opts.SaveDir
	if dir != "" {
		_, err := os.Stat(dir)
		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}
			if err := os.Mkdir(dir, 0755); err != nil {
				if !os.IsExist(err) {
					return err
				}
			}
			return nil
		}
	}
	if err := os.Mkdir("snaps", 0755); err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	opts.SaveDir = "snaps"
	return nil
}
func prepSegs(opts StoreOpt) (map[string]*mmap.MMap, error) {
	segMap := make(map[string]*mmap.MMap, 1)
	if opts.SnapShot.Rotate {
		index := time.Now().Format(time.DateOnly)
		mapped, err := newMmap(fmt.Sprintf("%s.dat", index), opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create new mmap: %s", err.Error())
		}
		segMap[index] = mapped
		return segMap, nil
	}
	mapped, err := newMmap("snapshot.dat", opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create new mmap: %s", err.Error())
	}
	segMap["snapshot.dat"] = mapped
	return segMap, nil
}
func newMmap(name string, opts StoreOpt) (*mmap.MMap, error) {
	dir := opts.SaveDir
	f, err := os.OpenFile(path.Join(dir, name), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open snapshot.dat: %s", err.Error())
	}
	if opts.SnapShot.MinSize == 0 {
		opts.SnapShot.MinSize = 4 * 1024 * 1024
	}
	if err := f.Truncate(opts.SnapShot.MinSize); err != nil {
		panic(err)
	}
	mapped, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		panic(err)
	}
	return &mapped, nil
}
func prepWal(opts StoreOpt) (*os.File, error) {
	f, err := os.OpenFile(path.Join(opts.SaveDir, "wal.log"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %s", err.Error())
	}
	return f, nil
}
