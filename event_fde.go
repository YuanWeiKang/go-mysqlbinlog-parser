package mysqlbinlog

type (
	FormatDescriptionFixedPart struct {
		BinaryLogFormatVersion uint16
		ServerVersion          [50]byte
		CreateTimestamp        uint32
		HeaderLength           uint8
	}
	FormatDescriptionEvent struct {
		Header    Header
		FixedPart FormatDescriptionFixedPart
	}
)

// BinaryLogFormatVersion + ServerVersion + CreateTimestamp + HeaderLength
const formatDescriptionEventLength = 57 // 2 + 50 + 4 + 1

func (f FormatDescriptionEvent) BinaryLogFormatVersion() int {
	return int(f.FixedPart.BinaryLogFormatVersion)
}

func (f FormatDescriptionEvent) ServerVersion() string {
	var s string
	for _, v := range f.FixedPart.ServerVersion {
		if v == 0x00 {
			break
		}
		s = s + string(v)
	}
	return s
}
