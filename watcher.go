package mysqlbinlog

import "github.com/fsnotify/fsnotify"

type EventWatcher struct {
	watcher *fsnotify.Watcher
	err     chan error
	write   chan struct{}
	rename  chan struct{}
	remove  chan struct{}
}

func NewEventWatcher(filename string) (*EventWatcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	err = watcher.Add(filename)
	if err != nil {
		return nil, err
	}
	return &EventWatcher{
		watcher: watcher,
		err:     make(chan error, 1),
		write:   make(chan struct{}, 1),
		rename:  make(chan struct{}, 1),
		remove:  make(chan struct{}, 1),
	}, nil
}

func (w *EventWatcher) start() {
	go func() {
		for {
			select {
			case event := <-w.watcher.Events:
				if event.Op&fsnotify.Write == fsnotify.Write {
					w.write <- struct{}{}
				}
				if event.Op&fsnotify.Rename == fsnotify.Rename {
					w.rename <- struct{}{}
				}
				if event.Op&fsnotify.Remove == fsnotify.Remove {
					w.remove <- struct{}{}
				}
			case err := <-w.watcher.Errors:
				w.err <- err
			}
		}
	}()
}
