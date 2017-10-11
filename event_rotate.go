package mysqlbinlog

type (
	RotateFixedPart struct {
		NextPosition uint32
	}
	RotateEvent struct {
		Header    Header
		FixedPart RotateFixedPart
		Variable  []byte
	}
)

const rotateFixedPartLength = 8

func (r RotateEvent) NextFile() string {
	return string(r.Variable[0 : len(r.Variable)-4])
}
