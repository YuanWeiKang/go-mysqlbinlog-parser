package mysqlbinlog

type (
	QueryFixedPart struct {
		ThreadID                  uint32
		QueryTime                 uint32
		DBNameLength              uint8
		ErrorCode                 uint16
		StatusVariableBlockLength uint16
	}
	QueryVariable struct {
		Status       []byte
		DBName       string
		SQLStatement string
	}
	QueryEvent struct {
		Header    Header
		FixedPart QueryFixedPart
		Variable  []byte
	}
)

// ThreadID, QueryTime + DBNameLength, ErrorCode, StatusVariableBlockLength
const queryFixedPartLength = 13

func (q QueryEvent) ThreadID() int64 {
	return int64(q.FixedPart.ThreadID)
}

func (q QueryEvent) QueryTime() int {
	return int(q.FixedPart.QueryTime)
}

func (q QueryEvent) ErrorCode() int {
	return int(q.FixedPart.ErrorCode)
}

func (q QueryEvent) parseQueryVariable() QueryVariable {
	pos := 0
	status := q.Variable[pos:int(q.FixedPart.StatusVariableBlockLength)]

	pos = pos + int(q.FixedPart.StatusVariableBlockLength)
	dbName := q.Variable[pos : pos+int(q.FixedPart.DBNameLength)]

	pos = pos + int(q.FixedPart.DBNameLength) + 1 // +1 : 0x00
	query := q.Variable[pos : len(q.Variable)-4]

	return QueryVariable{
		Status:       status,
		DBName:       string(dbName),
		SQLStatement: string(query),
	}
}

func (q QueryEvent) SQL() (string, string) {
	qv := q.parseQueryVariable()
	return qv.DBName, qv.SQLStatement
}
