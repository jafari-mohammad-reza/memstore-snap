package store

import "time"

type WalRec []byte
type StoreOpt struct {
	SnapShot SnapShotOpts
	Wal      WalOpts
	SaveDir  string
}

type RotationSpan time.Duration

const (
	Hourly RotationSpan = RotationSpan(time.Hour)
	Daily  RotationSpan = RotationSpan(time.Hour * 24)
	Weekly RotationSpan = RotationSpan((time.Hour * 24) * 7)
)

type SnapShotOpts struct {
	MinSize int64 // minimum size of segments 4mb by default
	Rotate  bool
	Span    RotationSpan
}

type WalOpts struct {
	Wal      bool
	WalSize  uint
	TruncWal bool // truncate wal in successful snapshot
	ChanSize int
}
