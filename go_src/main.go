package main

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	logRetentionDays = 4
	parentsUp        = 1
	logsDir          = "logs"
	logPattern       = ".log"
)

func main() {
	log := log.Default()
	root, err := filepath.Abs(".")
	if err != nil {
		log.Printf("Error locating root: %v\n", err)
		return
	}
	for i := 0; i < parentsUp; i++ {
		root = filepath.Dir(root)
	}

	logFiles := findLogFiles(root, logPattern, log)
	log.Printf("found %d log files", len(logFiles))

	logFilesToDelete := make([]string, 0, len(logFiles))
	currTime := time.Now()
	for _, f := range logFiles {
		info, err := os.Stat(f)
		if err != nil {
			log.Printf("Error fetching file info: %v\n", err)
		}
		if int(currTime.Sub(info.ModTime()).Hours()/24) > logRetentionDays {
			logFilesToDelete = append(logFilesToDelete, f)
		}
	}

	if len(logFilesToDelete) == 0 {
		return
	}

	log.Printf("%d log files will be deleted\n", len(logFilesToDelete))

	// concurrent deletion
	var wg sync.WaitGroup
	for i := 0; i < len(logFilesToDelete); i++ {
		wg.Add(1)
	}
	for _, f := range logFilesToDelete {
		go func() {
			defer wg.Done()
			log.Printf("deleting %s\n", f)
			os.Remove(f)

		}()
	}
	wg.Wait()
}

func findLogFiles(startPath, filePattern string, l *log.Logger) []string {
	var res []string

	var dfs func(p string)
	dfs = func(p string) {
		l.Printf("checking dir %s\n", p)
		absPath, err := filepath.Abs(p)
		if err != nil {
			l.Printf("error constructing filepath for %s: %v\n", p, err)
		}
		entries, err := os.ReadDir(absPath)
		if err != nil {
			l.Printf("error opening %s: %v\n", p, err)
		}
		for _, e := range entries {
			fullPath := filepath.Join(p, e.Name())
			if e.IsDir() {
				dfs(fullPath)
				continue
			}
			if strings.Contains(e.Name(), filePattern) {
				res = append(res, fullPath)
			}
		}
	}
	dfs(startPath)

	return res
}
