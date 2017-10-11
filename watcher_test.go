package mysqlbinlog

import (
	"errors"
	"os"
	"testing"
)

func TestWatchNewLine(t *testing.T) {

	f, err := os.Create("watch_newline_test.txt")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if f != nil {
			f.Close()
		}
		os.Remove(f.Name())
	}()

	ew, err := NewEventWatcher(f.Name())
	if err != nil {
		t.Error(err)
	}

	ew.start()
	exitCh := make(chan error, 1)
	go func() {
		select {
		case <-ew.write:
			exitCh <- nil
		case <-ew.rename:
			exitCh <- errors.New("Got rename event. The event should be fsnotify.Write")
		case <-ew.remove:
			exitCh <- errors.New("Got remove event. The event should be fsnotify.Write")
		case e := <-ew.err:
			exitCh <- e
		}
	}()

	f.WriteString("aa")

	err = <-exitCh
	if err != nil {
		t.Error(err)
	}
}

func TestWatchRename(t *testing.T) {
	f, err := os.Create("watch_rename_test.txt")
	renamedFile := "rename.txt"
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if f != nil {
			f.Close()
		}
		os.Remove(f.Name())
		os.Remove(renamedFile)
	}()

	ew, err := NewEventWatcher(f.Name())
	if err != nil {
		t.Error(err)
	}

	ew.start()
	exitCh := make(chan error, 1)
	go func() {
		select {
		case <-ew.write:
			exitCh <- errors.New("Got write event. The event should be fsnotify.Rename")
		case <-ew.rename:
			exitCh <- nil
		case <-ew.remove:
			exitCh <- errors.New("Got remove event. The event should be fsnotify.Rename")
		case e := <-ew.err:
			exitCh <- e
		}
	}()

	os.Rename(f.Name(), renamedFile)

	err = <-exitCh
	if err != nil {
		t.Error(err)
	}
}
func TestWatchRemove(t *testing.T) {
	f, err := os.Create("watch_remove_test.txt")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if f != nil {
			f.Close()
		}
		os.Remove(f.Name())
	}()

	ew, err := NewEventWatcher(f.Name())
	if err != nil {
		t.Error(err)
	}

	ew.start()
	exitCh := make(chan error, 1)
	go func() {
		select {
		case <-ew.write:
			exitCh <- errors.New("Got write event. The event should be fsnotify.Write")
		case <-ew.rename:
			exitCh <- errors.New("Got rename event. The event should be fsnotify.Write")
		case <-ew.remove:
			exitCh <- nil
		case e := <-ew.err:
			exitCh <- e
		}
	}()

	os.Remove(f.Name())

	err = <-exitCh
	if err != nil {
		t.Error(err)
	}
}
