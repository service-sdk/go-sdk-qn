package operation

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/fsnotify/fsnotify"
)

var (
	globalWatcher        *fsnotify.Watcher
	onceForGlobalWatcher sync.Once

	globalWatchedFiles sync.Map
	globalWatchedDirs  sync.Map
)

const QINIU_DISABLE_CONFIG_HOT_RELOADING_ENV = "QINIU_DISABLE_CONFIG_HOT_RELOADING"

func initGlobalWatcher() {
	var initWg sync.WaitGroup
	initWg.Add(1)
	go func() {
		var err error

		globalWatcher, err = fsnotify.NewWatcher()
		if err != nil {
			elog.Fatal(err)
			return
		}
		defer globalWatcher.Close()

		var eventsLoopWg sync.WaitGroup
		eventsLoopWg.Add(1)
		go func() {
			defer eventsLoopWg.Done()

			for {
				select {
				case event, ok := <-globalWatcher.Events:
					if !ok {
						return
					}
					const WRITE_OR_CREATE_MASK = fsnotify.Write | fsnotify.Create
					if event.Op&WRITE_OR_CREATE_MASK != 0 {
						pathChanged := filepath.Clean(event.Name)
						if _, watched := globalWatchedFiles.Load(pathChanged); watched {
							reloadCurrentConfigurable()
						}
					}
				case err, ok := <-globalWatcher.Errors:
					if !ok {
						return
					}
					elog.Warn("global watcher error:", err)
				}
			}
		}()
		initWg.Done()
		eventsLoopWg.Wait()
	}()
	initWg.Wait()
}

func ensureWatches(paths []string) error {
	if os.Getenv(QINIU_DISABLE_CONFIG_HOT_RELOADING_ENV) != "" {
		return nil
	}

	onceForGlobalWatcher.Do(initGlobalWatcher)

	toWatchPaths := make(map[string]struct{})
	var firstError error
	for _, path := range paths {
		toWatchPaths[filepath.Clean(path)] = struct{}{}
	}
	for path := range toWatchPaths {
		if err := _addToWatcher(path); err != nil {
			if firstError == nil {
				firstError = err
			}
		}
	}
	globalWatchedFiles.Range(func(key, _ interface{}) bool {
		path := key.(string)
		if _, exists := toWatchPaths[path]; !exists {
			if err := _removeFromWatcher(path); err != nil {
				if firstError == nil {
					firstError = err
				}
			}
		}
		return true
	})
	return firstError
}

func _addToWatcher(path string) error {
	if _, loaded := globalWatchedFiles.LoadOrStore(path, struct{}{}); !loaded {
		watchDir := filepath.Dir(path)
		initCounter := int64(0)
		gotCounter, loaded := globalWatchedDirs.LoadOrStore(watchDir, &initCounter)
		if !loaded {
			if err := globalWatcher.Add(watchDir); err != nil {
				globalWatchedDirs.Delete(watchDir)
				globalWatchedFiles.Delete(path)
				elog.Warn("add watch error:", watchDir, err)
				return err
			}
		}
		atomic.AddInt64(gotCounter.(*int64), 1)
	}
	return nil
}

func _removeFromWatcher(path string) (err error) {
	if _, loaded := globalWatchedFiles.LoadAndDelete(path); loaded {
		watchDir := filepath.Dir(path)
		if gotCounter, loaded := globalWatchedDirs.Load(watchDir); loaded {
			if atomic.AddInt64(gotCounter.(*int64), -1) <= 0 {
				if err = globalWatcher.Remove(watchDir); err != nil {
					elog.Warn("remove watch error:", watchDir, err)
				}
				globalWatchedDirs.Delete(watchDir)
			}
		}
	}
	return
}
