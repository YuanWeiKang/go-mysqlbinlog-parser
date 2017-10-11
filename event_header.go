package mysqlbinlog

import "time"

type Header struct {
	Timestamp    uint32
	TypeCode     uint8
	ServerID     uint32
	EventLength  uint32
	NextPosition uint32
	Flags        uint16
}

// Timestamp + TypeCode + ServerID + EventLength + NextPosition + Flangs
const eventHeaderLength = 19

func (h Header) UnixTimestamp() int64 {
	return int64(h.Timestamp)
}

func (h Header) Datetime() time.Time {
	return time.Unix(int64(h.Timestamp), 0)
}

func (h Header) Int64ServerID() int64 {
	return int64(h.ServerID)
}

func (h Header) Int64NextPosition() int64 {
	return int64(h.NextPosition)
}
