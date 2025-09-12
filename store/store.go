package store

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"
	"unsafe"

	"github.com/edsrzf/mmap-go"
	"github.com/klauspost/compress/zstd"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

type fileHeader struct {
	MagicByte [4]byte
	Version   uint32
	Size      uint64
	Checksum  [16]byte
	Reserved  [40]byte
}

const (
	headerSize = 64
	magicBytes = "STOR"
)

type Mapped struct {
	file *os.File
	mp   mmap.MMap
	mu   sync.RWMutex
	size uint64
}

type Store[T any] struct {
	opt      StoreOpt
	walFile  *os.File
	segFiles map[string]*Mapped // map of rotation period to file
	walChan  chan WalRec
	walMu    sync.Mutex
	encoder  *zstd.Encoder
	decoder  *zstd.Decoder
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
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("failed to create encoder: %s", err.Error())
	}
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create decoder: %w", err)
	}
	return &Store[T]{
		walMu:    sync.Mutex{},
		walFile:  wal,
		walChan:  make(chan WalRec, opts.Wal.WalSize),
		segFiles: segs,
		opt:      opts,
		encoder:  encoder,
		decoder:  decoder,
	}, nil
}
func (s *Store[T]) Stop() error {
	close(s.walChan)
	if s.encoder != nil {
		s.encoder.Close()
	}
	if s.decoder != nil {
		s.decoder.Close()
	}
	if err := s.walFile.Close(); err != nil {
		return err
	}

	for _, m := range s.segFiles {
		if err := m.Close(); err != nil {
			return err
		}
	}
	return nil
}
func (m *Mapped) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.mp != nil {
		if err := m.mp.Unmap(); err != nil {
			return err
		}
	}
	if m.file != nil {
		return m.file.Close()
	}
	return nil
}
func prepDir(opts StoreOpt) error {
	dir := opts.SaveDir
	if dir == "" {
		dir = "snaps"
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	if opts.SaveDir == "" {
		opts.SaveDir = dir
	}
	return nil
}
func prepSegs(opts StoreOpt) (map[string]*Mapped, error) {
	segMap := make(map[string]*Mapped, 1)
	if opts.SnapShot.Rotate {
		index := time.Now().Format(time.DateOnly)
		mp, err := newMmap(fmt.Sprintf("%s.dat", index), opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create new mmap: %s", err.Error())
		}
		segMap[index] = mp
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
	filePath := path.Join(dir, name)
	info, err := os.Stat(filePath)
	exists := err == nil
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", name, err)
	}
	initialSize := opts.SnapShot.MinSize
	if initialSize == 0 {
		initialSize = 1024 * 1024
	}
	if !exists || info.Size() < int64(headerSize) {
		if err := f.Truncate(int64(initialSize)); err != nil {
			f.Close()
			return nil, fmt.Errorf("failed to truncate: %w", err)
		}
	}
	if !exists || info.Size() < int64(headerSize) {
		if err := f.Truncate(int64(initialSize)); err != nil {
			f.Close()
			return nil, fmt.Errorf("failed to truncate: %w", err)
		}
	}
	mapped, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to mmap: %w", err)
	}
	m := &Mapped{
		file: f,
		mp:   mapped,
	}
	if !exists {
		m.initializeHeader()
	} else {
		if err := m.readHeader(); err != nil {
			m.Close()
			return nil, err
		}
	}
	return m, nil
}
func (m *Mapped) initializeHeader() {
	header := fileHeader{}
	copy(header.MagicByte[:], magicBytes)
	header.Version = 1
	header.Size = 0

	headerBytes := (*[headerSize]byte)(unsafe.Pointer(&header))
	copy(m.mp[:headerSize], headerBytes[:])
}
func (m *Mapped) readHeader() error {
	if len(m.mp) < headerSize {
		return fmt.Errorf("file too small for header")
	}

	header := (*fileHeader)(unsafe.Pointer(&m.mp[0]))
	if string(header.MagicByte[:]) != magicBytes {
		return fmt.Errorf("invalid magic bytes")
	}

	m.size = header.Size
	return nil
}
func (m *Mapped) writeHeader(dataSize uint64) error {
	if len(m.mp) < headerSize {
		return fmt.Errorf("mmap too small for header")
	}
	header := (*fileHeader)(unsafe.Pointer(&m.mp[0]))
	header.Size = dataSize
	checksum, err := m.calcChecksum(dataSize)
	if err != nil {
		return err
	}
	copy(header.Checksum[:], checksum)

	return nil
}
func (m *Mapped) calcChecksum(dataSize uint64) ([]byte, error) {
	if headerSize+dataSize > uint64(len(m.mp)) {
		return nil, fmt.Errorf("data size exceeds mmap bounds")
	}

	hash := md5.New()

	data := m.mp[headerSize : headerSize+dataSize]
	hash.Write(data)

	return hash.Sum(nil), nil
}
func prepWal(opts StoreOpt) (*os.File, error) {
	f, err := os.OpenFile(path.Join(opts.SaveDir, "wal.log"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %s", err.Error())
	}
	return f, nil
}

func (s *Store[T]) Snapshot(entries []T) error {
	data, err := msgpack.Marshal(entries)
	if err != nil {
		return fmt.Errorf("failed to encode entries")
	}
	compressed := s.encoder.EncodeAll(data, nil)
	compressedSize := uint64(len(compressed))
	seg, err := s.getSeg()
	if err != nil {
		return err
	}
	seg.mu.Lock()
	defer seg.mu.Unlock()
	requiredSize := headerSize + compressedSize
	if uint64(len(seg.mp)) < requiredSize {
		if err := seg.mp.Unmap(); err != nil {
			return fmt.Errorf("failed to unmap segment mmap")
		}
		newSize := uint64(len(seg.mp)) * 3 / 2
		if newSize < requiredSize {
			newSize = requiredSize + 1024*1024
		}
		if err := seg.file.Truncate(int64(newSize)); err != nil {
			return fmt.Errorf("failed to grow segment file size")
		}
		seg.mp, err = mmap.Map(seg.file, mmap.RDWR, 0)
		if err != nil {
			return err
		}
	}
	copy(seg.mp[headerSize:], compressed)
	seg.writeHeader(compressedSize)
	seg.size = compressedSize
	return seg.mp.Flush()
}

func (s *Store[T]) SnapshotBinary(entries []T, writer func(w io.Writer, entry T) error) error {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, uint64(len(entries)))
	for _, entry := range entries {
		if err := writer(&buf, entry); err != nil {
			return err
		}
	}
	compressed := s.encoder.EncodeAll(buf.Bytes(), nil)
	compressedSize := uint64(len(compressed))
	seg, err := s.getSeg()
	if err != nil {
		return err
	}
	seg.mu.Lock()
	defer seg.mu.Unlock()
	requiredSize := headerSize + compressedSize
	if uint64(len(seg.mp)) < requiredSize {
		if err := seg.mp.Unmap(); err != nil {
			return fmt.Errorf("failed to unmap segment mmap")
		}
		newSize := uint64(len(seg.mp)) * 3 / 2
		if newSize < requiredSize {
			newSize = requiredSize + 1024*1024
		}
		if err := seg.file.Truncate(int64(newSize)); err != nil {
			return fmt.Errorf("failed to grow segment file size")
		}
		seg.mp, err = mmap.Map(seg.file, mmap.RDWR, 0)
		if err != nil {
			return err
		}
	}
	copy(seg.mp[headerSize:], compressed)
	seg.writeHeader(compressedSize)
	seg.size = compressedSize
	return seg.mp.Flush()
}

func (s *Store[T]) LoadSnapshot(dest *[]T) error {
	seg, err := s.getSeg()
	if err != nil {
		return err
	}

	seg.mu.RLock()
	defer seg.mu.RUnlock()

	if seg.size == 0 {
		return fmt.Errorf("no data in snapshot")
	}
	compressed := seg.mp[headerSize : headerSize+seg.size]
	decompressed, err := s.decoder.DecodeAll(compressed, nil)
	if err != nil {
		return fmt.Errorf("failed to decompress: %w", err)
	}
	if err := msgpack.Unmarshal(decompressed, dest); err != nil {
		return fmt.Errorf("failed to unmarshal: %w", err)
	}
	return nil
}
func (s *Store[T]) getSeg() (*Mapped, error) {
	if s.opt.SnapShot.Rotate {
		index := time.Now().Format("2006-01-02")
		if seg, ok := s.segFiles[index]; ok {
			return seg, nil
		}

		seg, err := newMmap(fmt.Sprintf("%s.dat", index), s.opt)
		if err != nil {
			return nil, err
		}
		s.segFiles[index] = seg
		return seg, nil
	}

	if seg, ok := s.segFiles["snapshot"]; ok {
		return seg, nil
	}

	seg, err := newMmap("snapshot.dat", s.opt)
	if err != nil {
		return nil, err
	}
	s.segFiles["snapshot"] = seg
	return seg, nil
}
