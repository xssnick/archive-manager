package packsplit

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

const (
	Magic      uint32 = 0xae8fdd01
	FileMarker uint16 = 0x1e8b
)

var (
	ErrMagicMismatch  = errors.New("invalid magic header")
	ErrMarkerMismatch = errors.New("invalid file marker")
)

type Entry struct {
	Name string
	Data []byte
}

func ReadPackage(r io.Reader) ([]Entry, error) {
	var magic uint32
	if err := binary.Read(r, binary.LittleEndian, &magic); err != nil {
		return nil, err
	}
	if magic != Magic {
		return nil, ErrMagicMismatch
	}

	var entries []Entry
	for {
		var marker uint16
		if err := binary.Read(r, binary.LittleEndian, &marker); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if marker != FileMarker {
			return nil, ErrMarkerMismatch
		}

		var nameLen uint16
		if err := binary.Read(r, binary.LittleEndian, &nameLen); err != nil {
			return nil, err
		}

		var size uint32
		if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
			return nil, err
		}

		nameBuf := make([]byte, nameLen)
		if _, err := io.ReadFull(r, nameBuf); err != nil {
			return nil, err
		}

		data := make([]byte, size)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, err
		}

		entries = append(entries, Entry{
			Name: string(nameBuf),
			Data: data,
		})
	}
	return entries, nil
}

func WritePackage(w io.Writer, entries []Entry) error {
	if err := binary.Write(w, binary.LittleEndian, Magic); err != nil {
		return err
	}
	for _, e := range entries {
		if err := binary.Write(w, binary.LittleEndian, FileMarker); err != nil {
			return err
		}
		if len(e.Name) > 0xffff {
			return errors.New("filename too long")
		}
		if err := binary.Write(w, binary.LittleEndian, uint16(len(e.Name))); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, uint32(len(e.Data))); err != nil {
			return err
		}
		if _, err := w.Write([]byte(e.Name)); err != nil {
			return err
		}
		if _, err := w.Write(e.Data); err != nil {
			return err
		}
	}
	return nil
}

// regex: ..._(wc,shard,seq):...
var nameRE = regexp.MustCompile(`^[a-z]+_\((-?\d+),([0-9a-fA-F]+),(\d+)\):`)

type key struct {
	wc    int64
	shard string
	seq   int64
}

func parseKey(name string) (key, error) {
	m := nameRE.FindStringSubmatch(name)
	if m == nil {
		return key{}, fmt.Errorf("invalid filename: %s", name)
	}
	wc, _ := strconv.ParseInt(m[1], 10, 64)
	shard := strings.ToLower(m[2])
	seq, _ := strconv.ParseInt(m[3], 10, 64)
	return key{wc: wc, shard: shard, seq: seq}, nil
}

func SplitByFiles(idx int, entries []Entry) (map[string][]Entry, error) {
	result := make(map[string][]Entry)
	for _, e := range entries {
		k, err := parseKey(e.Name)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", err, e.Name)
		}
		var fname string
		if k.wc == -1 {
			fname = fmt.Sprintf("archive.%05d.pack", idx)
		} else {
			fname = fmt.Sprintf("archive.%05d.%d:8000000000000000.pack", idx, k.wc)
		}
		result[fname] = append(result[fname], e)
	}
	return result, nil
}
