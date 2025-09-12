package store

import (
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
)

type Mapped struct {
	file *os.File
	mp   *mmap.MMap
}

// there should be options as rotation, multiple snapshot files
type Store[T any] struct {
	opt      StoreOpt
	walFile  *os.File
	segFiles map[string]*Mapped // map of rotation period to file
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
		if err := f.file.Close(); err != nil {
			return err
		}
		if err := f.mp.Unmap(); err != nil {
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
func prepSegs(opts StoreOpt) (map[string]*Mapped, error) {
	segMap := make(map[string]*Mapped, 1)
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

func newMmap(name string, opts StoreOpt) (*Mapped, error) {
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
	return &Mapped{
		file: f,
		mp:   &mapped,
	}, nil
}
func prepWal(opts StoreOpt) (*os.File, error) {
	f, err := os.OpenFile(path.Join(opts.SaveDir, "wal.log"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %s", err.Error())
	}
	return f, nil
}

func (s *Store[T]) Snapshot(entries []T) error {
	seg, err := s.getSeg()
	if err != nil {
		return err
	}
	seg.mp.Lock()
	if !s.opt.SnapShot.Rotate {
		// as we dont have rotation we truncate previous snapshot
		seg.file.Write([]byte{}) // truncate previous snapshot
	}
	seg.mp.Unlock()
	defer seg.mp.Unmap()
	defer seg.file.Close()
	gzWriter := gzip.NewWriter(seg.file)
	defer gzWriter.Close()
	encoder := gob.NewEncoder(gzWriter)
	if err := encoder.Encode(entries); err != nil {
		return fmt.Errorf("failed to encode entries data: %w", err)
	}
	seg.mp.Flush()
	return nil
}

func (s *Store[T]) LoadSnapshot(dest *T) error {
	seg, err := s.getSeg()
	if err != nil {
		return erra
	}
	gzReader, err := gzip.NewReader(seg.file)
	if err != nil {
		return err
	}
	defer gzReader.Close()

	decoder := gob.NewDecoder(gzReader)
	if err := decoder.Decode(dest); err != nil {
		return fmt.Errorf("failed to decode snapshot data: %w", err)
	}
	return nil
}
func (s *Store[T]) getSeg() (*Mapped, error) {
	var seg *Mapped
	if s.opt.SnapShot.Rotate {
		name := fmt.Sprintf("%d.dat", len(s.segFiles)+1)
		segF, ok := s.segFiles[name]
		if !ok {
			segF, err := newMmap(name, s.opt)
			if err != nil {
				return nil, fmt.Errorf("failed to create new snapshot file: %s", err.Error())
			}
			s.segFiles[name] = segF
			seg = segF
		}
		seg = segF
	} else {
		name := "snapshot.dat"
		segF, ok := s.segFiles[name]
		if !ok {
			segF, err := newMmap(name, s.opt)
			if err != nil {
				return nil, fmt.Errorf("failed to create new snapshot file: %s", err.Error())
			}
			s.segFiles[name] = segF
			seg = segF
		}
		seg = segF
	}
	return seg, nil
}
