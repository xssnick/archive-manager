package fs

import (
	"context"
	"fmt"
	"github.com/rs/zerolog/log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type FileInfo struct {
	Name string
	Size uint64
}

// PackInfo holds the directory name and shard filenames for a given sequence number.
type PackInfo struct {
	Seqno uint32
	List  []FileInfo
}

// Watcher monitors filesystem directories and indexes archive pack files safely across goroutines.
type Watcher struct {
	globalCtx context.Context
	cancel    context.CancelFunc
	root      string               // root directory for watching
	mu        sync.RWMutex         // guards access to index and maxPack
	index     map[uint32]*PackInfo // map of sequence number to PackInfo
	maxDir    uint64               // highest directory sequence number seen
	maxPack   uint64               // highest pack sequence number seen
	logger    zerolog.Logger

	scanCompleted context.Context
	scanComplete  context.CancelFunc
}

// NewWatcher creates a new Watcher for the given root directory, using the provided zerolog instance.
func NewWatcher(root string, logger zerolog.Logger) *Watcher {
	ctx, cancel := context.WithCancel(context.Background())
	ctxComplete, cancelComplete := context.WithCancel(context.Background())
	return &Watcher{
		globalCtx:     ctx,
		cancel:        cancel,
		root:          root,
		index:         make(map[uint32]*PackInfo),
		logger:        logger,
		scanCompleted: ctxComplete,
		scanComplete:  cancelComplete,
	}
}

// Watch starts an initial scan and then polls for new directories every 5 seconds.
func (w *Watcher) Watch() {
	// initial full directory scan
	maxDir, err := w.initialScan()
	if err != nil {
		w.logger.Fatal().Err(err).Msg("initial scan error")
	}
	w.maxDir = maxDir

	w.scanComplete()

	// ticker for incremental scans
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.globalCtx.Done():
			return
		case <-ticker.C:
			newMaxDir, err := w.scanNewDirs()
			if err != nil {
				w.logger.Error().Err(err).Msg("scan new dirs error")
				continue
			}
			if newMaxDir > w.maxDir {
				w.maxDir = newMaxDir
			}
		}
	}
}

// initialScan indexes all existing directories and pack files once.
func (w *Watcher) initialScan() (uint64, error) {
	dirMap, nums, err := w.listDirs()
	if err != nil {
		return 0, err
	}

	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })

	var maxDir uint64
	for _, dirSeq := range nums {
		dirName := dirMap[dirSeq]
		if err := w.scanFiles(dirName, true); err != nil {
			w.logger.Error().Err(err).Str("folder", dirName).Msg("scanFiles error during initial scan")
		}
		if dirSeq > maxDir {
			maxDir = dirSeq
		}
	}

	return maxDir, nil
}

// scanNewDirs indexes only directories with sequence number >= maxDir.
func (w *Watcher) scanNewDirs() (uint64, error) {
	dirMap, nums, err := w.listDirs()
	if err != nil {
		return w.maxDir, err
	}

	var fresh []uint64
	for _, seq := range nums {
		if seq >= w.maxDir {
			fresh = append(fresh, seq)
		}
	}

	if len(fresh) == 0 {
		return w.maxDir, nil
	}

	sort.Slice(fresh, func(i, j int) bool { return fresh[i] < fresh[j] })

	newMaxDir := w.maxDir
	for _, seq := range fresh {
		dirName := dirMap[seq]
		if err := w.scanFiles(dirName, false); err != nil {
			w.logger.Error().Err(err).Str("folder", dirName).Msg("scanFiles error during incremental scan")
		}
		if seq > newMaxDir {
			newMaxDir = seq
		}
	}

	return newMaxDir, nil
}

// listDirs returns a mapping of directory sequence numbers to directory names and a slice of all sequence numbers found.
func (w *Watcher) listDirs() (map[uint64]string, []uint64, error) {
	entries, err := os.ReadDir(w.root)
	if err != nil {
		return nil, nil, err
	}

	dirs := make(map[uint64]string)
	var nums []uint64

	for _, e := range entries {
		select {
		case <-w.globalCtx.Done():
			return nil, nil, context.Canceled
		default:
		}

		if !e.IsDir() || !strings.HasPrefix(e.Name(), "arch") {
			continue
		}

		seqStr := strings.TrimPrefix(e.Name(), "arch")
		seq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			continue
		}

		dirs[seq] = e.Name()
		nums = append(nums, seq)
	}

	return dirs, nums, nil
}

// scanFiles indexes all archive and shard pack files in the given directory.
func (w *Watcher) scanFiles(dirName string, initial bool) error {
	select {
	case <-w.globalCtx.Done():
		return context.Canceled
	default:
	}

	dirPath := filepath.Join(w.root, dirName)
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}

	for _, f := range files {
		if f.IsDir() {
			continue
		}

		name := f.Name()
		if !strings.HasPrefix(name, "archive.") || !strings.HasSuffix(name, ".pack") {
			continue
		}

		// Remove prefix 'archive.' and suffix '.pack'
		inner := strings.TrimSuffix(strings.TrimPrefix(name, "archive."), ".pack")
		var seqStr string
		if idx := strings.Index(inner, "."); idx >= 0 {
			// Shard file: e.g., "47257082.0:2000000000000000"
			seqStr = inner[:idx]
		} else {
			// Main archive file
			seqStr = inner
		}

		seq, err := strconv.ParseUint(seqStr, 10, 32)
		if err != nil {
			continue
		}

		infoFile, err := f.Info()
		if err != nil {
			continue
		}
		size := uint64(infoFile.Size())

		w.mu.Lock()
		pi := w.index[uint32(seq)]
		if pi == nil {
			pi = &PackInfo{
				Seqno: uint32(seq),
			}
			if !initial {
				log.Info().Uint64("seqno", seq).Msg("new pack discovered")
			}
		}
		name = filepath.Join(w.root, dirName, name)

		found := false
		for i, file := range pi.List {
			if file.Name == name {
				pi.List[i].Size = size
				found = true
				break
			}
		}

		if !found {
			pi.List = append(pi.List, FileInfo{
				Name: name,
				Size: size,
			})
		}

		// Update maxPack if sequence is newer
		if seq > w.maxPack {
			w.maxPack = seq
		}
		w.index[uint32(seq)] = pi
		w.mu.Unlock()
	}

	return nil
}

// GetLatestPack returns the highest pack sequence number seen so far.
func (w *Watcher) GetLatestPack() uint64 {
	<-w.scanCompleted.Done()

	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.maxPack
}

var ErrZeroPack = fmt.Errorf("zero pack")

// FindPack returns full paths to the main archive and any shard files for the given sequence number.
func (w *Watcher) FindPack(seq uint32) (*PackInfo, error) {
	<-w.scanCompleted.Done()

	var ok bool
	var info *PackInfo
	w.mu.RLock()
	for { // find pack in which we have this seqno
		info, ok = w.index[seq]
		if ok || seq == 0 {
			break
		}

		seq--
	}
	w.mu.RUnlock()

	if !ok {
		return nil, ErrZeroPack
	}

	return info, nil
}

func (p *PackInfo) Files() []string {
	var res []string
	for _, info := range p.List {
		res = append(res, info.Name)
	}
	return res
}
