package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	mysqlbinlog "github.com/hiroakis/go-mysqlbinlog-parser"
)

type Query struct {
	Timestamp    int64  `json:"timestamp"`
	NextPosition int64  `json:"next_position"`
	ServerID     int64  `json:"server_id"`
	ThreadID     int64  `json:"thread_id"`
	QueryTime    int    `json:"query_time"`
	ErrorCode    int    `json:"error_code"`
	DBName       string `json:"db_name"`
	SQLStatement string `json:"sql"`
}

var reader *mysqlbinlog.Reader

var (
	binlog         string
	startPos       int64
	output         string
	follow         bool
	followNextFile bool
)

func read(events chan interface{}) (string, error) {

	var o io.Writer
	if output == "" {
		o = os.Stdout
	} else {
		f, err := os.Create(output)
		if err != nil {
			return "", err
		}
		o = f
	}

	writer := bufio.NewWriter(o)

	for event := range events {
		switch ev := event.(type) {
		case mysqlbinlog.QueryEvent:

			if reader.CurrentBinlog() == binlog && ev.Header.Int64NextPosition() < startPos {
				continue
			}

			dbName, sqlStatement := ev.SQL()
			q := Query{
				Timestamp:    ev.Header.UnixTimestamp(),
				NextPosition: ev.Header.Int64NextPosition(),
				ServerID:     ev.Header.Int64ServerID(),
				ThreadID:     ev.ThreadID(),
				QueryTime:    ev.QueryTime(),
				ErrorCode:    ev.ErrorCode(),
				DBName:       dbName,
				SQLStatement: sqlStatement,
			}
			fmt.Printf("%+v\n", q)

			b, err := json.Marshal(q)
			if err != nil {
				return "", err
			}
			writer.Write(b)
			writer.Write([]byte("\n"))
			writer.Flush()

		case mysqlbinlog.RotateEvent:
			return ev.NextFile(), nil
		case nil:
			break
		}
	}
	close(events)
	return "", nil
}

func isExist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

func main() {
	flag.StringVar(&binlog, "file", "", "")
	flag.Int64Var(&startPos, "p", 0, "")
	flag.StringVar(&output, "o", "", "")
	flag.BoolVar(&follow, "f", false, "")
	flag.BoolVar(&followNextFile, "F", false, "")
	flag.Parse()
	if binlog == "" {
		fmt.Println("-file is required")
		return
	}

	var (
		err error
	)

	binlogDir := filepath.Dir(binlog)
	binlogPath, err := filepath.Abs(binlog)
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		reader, err = mysqlbinlog.NewReader(binlogPath, follow)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer reader.Close()

		events := reader.Each()
		nextFile, err := read(events)
		if err != nil {
			fmt.Println(err)
			break
		}
		if nextFile != "" && follow && followNextFile {
			binlogPath = filepath.Join(binlogDir, nextFile)
			reader.Close()
			for {
				if isExist(binlogPath) {
					break
				}
				time.Sleep(1000 * time.Millisecond)
			}

			reader, err = mysqlbinlog.NewReader(binlogPath, follow)
			if err != nil {
				fmt.Println(err)
				break
			}
			events = reader.Each()
		}
	}
}
