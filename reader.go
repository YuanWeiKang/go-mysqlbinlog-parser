package mysqlbinlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// 0xfe 0x62 0x96 0x6e
var magicNumber = []byte{byte(254), byte(98), byte(105), byte(110)}

type Reader struct {
	binlog   *os.File
	position int64
	watcher  *EventWatcher
}

func NewReader(binlog string, tail bool) (*Reader, error) {
	file, err := os.Open(binlog)
	if err != nil {
		return nil, err
	}

	// read magic number
	magicNumberBuf := make([]byte, len(magicNumber))
	file.Read(magicNumberBuf)
	if !bytes.Equal(magicNumberBuf, magicNumber) {
		return nil, errors.New("No magic number found. MySQL binlog should contain '0xfe 0x62 0x96 0x6e'")
	}

	var w *EventWatcher
	if tail {
		w, err = NewEventWatcher(binlog)
		if err != nil {
			return nil, err
		}
		w.start()
	} else {
		w = nil
	}

	reader := &Reader{
		binlog:   file,
		position: int64(len(magicNumber)),
		watcher:  w,
	}

	return reader, nil
}

func (r *Reader) Close() {
	if r.binlog != nil {
		r.binlog.Close()
	}
}

func (r *Reader) CurrentPosition() int64 {
	return r.position
}

func (r *Reader) read(dst interface{}, size int) error {
	buf := make([]byte, size)
	_, err := r.binlog.Read(buf)
	if err == io.EOF {
		return err
	}
	return binary.Read(bytes.NewBuffer(buf), binary.LittleEndian, dst)
}

func (r *Reader) readVariable(size int) ([]byte, error) {
	buf := make([]byte, size)
	_, err := r.binlog.Read(buf)
	return buf, err
}

func (r *Reader) skip(offset int64) error {
	_, err := r.binlog.Seek(offset, 1)
	return err
}

func (r *Reader) isEOF(err error) bool {
	if err == io.EOF {
		return true
	}
	return false
}

func (r *Reader) isTailMode() bool {
	if r.watcher == nil {
		return false
	}
	return true
}

func (r *Reader) waitForEvent() error {
	var err error
	select {
	case <-r.watcher.write:
		err = nil
	case <-r.watcher.rename:
		err = fmt.Errorf("%s has been lost. The file may be renamed", r.binlog.Name())
	case <-r.watcher.remove:
		err = fmt.Errorf("%s has been lost. The file may be removed", r.binlog.Name())
	case eerr := <-r.watcher.err:
		err = eerr
	}
	return err
}

func (r *Reader) Each() chan interface{} {
	eventCh := make(chan interface{}, 1)

	go func() {
		var pos int64
		for {
			var header Header
			err := r.read(&header, eventHeaderLength)
			if err != nil {
				if r.isEOF(err) && r.isTailMode() {
					if err := r.waitForEvent(); err != nil {
						fmt.Println(err)
						break
					}
					continue
				} else {
					fmt.Println(err)
					break
				}
			}

			eventDataLength := int(header.EventLength) - eventHeaderLength
			pos = int64(header.NextPosition)

			switch header.TypeCode {
			case stopEventTypeCode:
				break
			case queryEventTypeCode:
				var queryFixedPart QueryFixedPart
				if err := r.read(&queryFixedPart, queryFixedPartLength); err != nil {
					fmt.Println(err)
					break
				}

				// variableSize = EventLength - (eventHeaderLength + queryEventFixedDataPartLength)
				variableSize := int(header.EventLength) - (eventHeaderLength + queryFixedPartLength)
				queryVariable, err := r.readVariable(variableSize)
				if err != nil && !r.isEOF(err) {
					fmt.Println(err)
					break
				}

				eventCh <- QueryEvent{
					Header:    header,
					FixedPart: queryFixedPart,
					Variable:  queryVariable,
				}
			case rotateEventTypeCode:
				var rotateFixedPart RotateFixedPart
				if err := r.read(&rotateFixedPart, rotateFixedPartLength); err != nil {
					fmt.Println(err)
					break
				}

				// variableSize := int(header.EventLength) - rotateFixedPartLength
				variableSize := int(header.EventLength) - (eventHeaderLength + rotateFixedPartLength)
				rotateVariable, err := r.readVariable(variableSize)
				if err != nil && !r.isEOF(err) {
					fmt.Println(err)
					break
				}

				eventCh <- RotateEvent{
					Header:    header,
					FixedPart: rotateFixedPart,
					Variable:  rotateVariable,
				}
			case formatDescriptionEventTypeCode:
				var formatDescriptionFixedPart FormatDescriptionFixedPart
				if err := r.read(&formatDescriptionFixedPart, formatDescriptionEventLength); err != nil {
					fmt.Println(err)
					break
				}

				postHeaderLength := int(eventDataLength) - formatDescriptionEventLength
				if err := r.skip(int64(postHeaderLength)); err != nil {
					if !r.isEOF(err) {
						fmt.Println(err)
					}
					break
				}

				eventCh <- FormatDescriptionEvent{
					Header:    header,
					FixedPart: formatDescriptionFixedPart,
				}
			default:
				if err := r.skip(int64(eventDataLength)); err != nil {
					if !r.isEOF(err) {
						fmt.Println(err)
					}
					break
				}
			}
			r.position = pos
		}
		close(eventCh)
	}()

	return eventCh
}
