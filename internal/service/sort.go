package service

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var re = regexp.MustCompile(`archive\.(\d+)(?:\.0\:([0-9A-Za-z]+))?\.pack$`)

type fileInfo struct {
	raw     string
	baseNum int64
	hasTail bool
	tailNum int64
	tailStr string
}

func parseFile(path string) (fileInfo, error) {
	name := filepath.Base(path)
	m := re.FindStringSubmatch(name)
	if m == nil {
		return fileInfo{}, fmt.Errorf("unknown: %s", name)
	}

	base, _ := strconv.ParseInt(m[1], 10, 64)

	fi := fileInfo{
		raw:     path,
		baseNum: base,
	}

	if m[2] != "" {
		fi.hasTail = true
		fi.tailStr = m[2]
		digits := regexp.MustCompile(`\D`).ReplaceAllString(fi.tailStr, "")
		if digits != "" {
			fi.tailNum, _ = strconv.ParseInt(digits, 10, 64)
		}
	}

	return fi, nil
}

func sortFiles(list []string) ([]string, error) {
	files := make([]fileInfo, len(list))
	for i, p := range list {
		f, err := parseFile(p)
		if err != nil {
			return nil, err
		}
		files[i] = f
	}

	sort.Slice(files, func(i, j int) bool {
		a, b := files[i], files[j]

		if a.hasTail != b.hasTail {
			return !a.hasTail
		}
		if a.baseNum != b.baseNum {
			return a.baseNum < b.baseNum
		}
		if a.tailNum != b.tailNum {
			return a.tailNum < b.tailNum
		}
		return strings.Compare(a.tailStr, b.tailStr) < 0
	})

	out := make([]string, len(files))
	for i, f := range files {
		out[i] = f.raw
	}
	return out, nil
}
