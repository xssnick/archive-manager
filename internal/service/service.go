package service

import (
	"archive-manager/pkg/control"
	"archive-manager/pkg/fs"
	"archive-manager/pkg/index"
	"archive-manager/pkg/packsplit"
	"archive-manager/pkg/state"
	"archive-manager/pkg/storage"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Service struct {
	node      *control.Client
	fsWatcher *fs.Watcher
	idx       *index.Index
	gen       *state.Generator
	storage   *storage.Client

	latestSeqnoProcessed uint32
	latestPackProcessed  *uint32
	stateEveryBlocksNum  uint32

	sizePerBag    uint64
	bagPrefix     string
	initialStates []uint32

	closerCtx context.Context
	closer    context.CancelFunc
}

func NewService(node *control.Client, fsWatcher *fs.Watcher, idx *index.Index, gen *state.Generator, storage *storage.Client, bagPrefix string, sizePerBlocksBag uint64, stateEveryBlocksNum uint32, initialStates []uint32) *Service {
	closerCtx, closer := context.WithCancel(context.Background())
	return &Service{
		idx:                 idx,
		node:                node,
		gen:                 gen,
		storage:             storage,
		bagPrefix:           bagPrefix,
		sizePerBag:          sizePerBlocksBag,
		stateEveryBlocksNum: stateEveryBlocksNum,
		initialStates:       initialStates,
		fsWatcher:           fsWatcher,
		closerCtx:           closerCtx,
		closer:              closer,
	}
}

func (s *Service) StartBlocks() {
	log.Info().Msg("starting blocks worker")
	wait := time.Duration(0)

	var packs = make(map[uint32]*fs.PackInfo)
	s.latestSeqnoProcessed = s.idx.GetLatestBlock().ToPack
	if s.latestSeqnoProcessed > 0 {
		pc := s.latestSeqnoProcessed
		s.latestPackProcessed = &pc
	}

	log.Info().Uint32("seqno", s.latestSeqnoProcessed).Msg("starting scan block packs from seqno")

	for {
		select {
		case <-s.closerCtx.Done():
			log.Info().Msg("stopping blocks worker")
			return
		case <-time.After(wait):
			wait = 2 * time.Second
			log.Debug().
				Uint32("last_processed", s.latestSeqnoProcessed).
				Msg("waiting for new seqno")
		}

		lastSeqno, err := s.latestNodeSeqnoFinished()
		if err != nil {
			log.Error().Err(err).Msg("failed to get latest node seqno")
			continue
		}

		if s.latestSeqnoProcessed < lastSeqno {
			log.Info().
				Uint32("last_finished_seqno", lastSeqno).
				Msg("ready to process")
		}

		for s.latestSeqnoProcessed < lastSeqno {
			pack, err := s.fsWatcher.FindPack(s.latestSeqnoProcessed)
			if err != nil {
				if errors.Is(err, fs.ErrZeroPack) {
					s.latestSeqnoProcessed++
					continue
				}
				log.Error().
					Err(err).
					Uint32("seqno", s.latestSeqnoProcessed).
					Msg("failed to fetch pack files")
				break
			}

			if s.latestPackProcessed != nil && *s.latestPackProcessed >= pack.Seqno {
				s.latestSeqnoProcessed++
				continue
			}

			if _, has := packs[pack.Seqno]; !has {
				packs[pack.Seqno] = pack

				var totalSz uint64
				for _, info := range packs {
					for i := range info.List {
						totalSz += info.List[i].Size
					}
				}

				if totalSz >= s.sizePerBag {
					var filesForBag []string
					var fromPack, toPack *uint32
					for _, info := range packs {
						if fromPack == nil || *fromPack > info.Seqno {
							fromPack = &info.Seqno
						}
						if toPack == nil || *toPack < info.Seqno {
							toPack = &info.Seqno
						}
						filesForBag = append(filesForBag, info.Files()...)
					}

					filesForBag, err = sortFiles(filesForBag)
					if err != nil {
						log.Error().Err(err).Msg("failed to sort files")
						break
					}

					log.Info().
						Int("files", len(filesForBag)).
						Str("first_file", filesForBag[0]).
						Str("last_file", filesForBag[len(filesForBag)-1]).
						Uint64("size", totalSz).
						Uint32("from_pack", *fromPack).
						Uint32("to_pack", *toPack).
						Msg("making bag")

					dir := filepath.Dir(filepath.Dir(filesForBag[0]))
					ctx, cancel := context.WithTimeout(s.closerCtx, 1*time.Hour)
					bagId, err := s.storage.CreateBag(ctx, dir, fmt.Sprintf("%s Block packs from %d to %d", s.bagPrefix, *fromPack, *toPack), filesForBag)
					cancel()
					if err != nil {
						log.Error().Err(err).Msg("failed to create bag")
						delete(packs, pack.Seqno)
						break
					}

					s.idx.AddBlockSet(index.BlockSet{
						FromPack: *fromPack,
						ToPack:   *toPack,
						Bag:      hex.EncodeToString(bagId),
					})
					if err = s.idx.Save(); err != nil {
						log.Error().Err(err).Msg("failed to save index")
						delete(packs, pack.Seqno)
						break
					}

					packs = make(map[uint32]*fs.PackInfo)

					pc := *toPack
					s.latestPackProcessed = &pc
				}
			}
			s.latestSeqnoProcessed++
		}
	}
}

func (s *Service) StartStates() {
	log.Info().Msg("starting states worker")

	wait := time.Duration(0)

	skipPrev := false

	for {
		select {
		case <-s.closerCtx.Done():
			log.Info().Msg("stopping states worker")
			return
		case <-time.After(wait):
			wait = 5 * time.Minute
		}

		keyBlockSeqno, mandatory, err := s.latestKeyBlockForNextState()
		if err != nil {
			log.Error().Err(err).Msg("failed to get latest node key block seqno")
			wait = 5 * time.Second
			continue
		}

		var prevSeqno *uint32
		var lastAt uint32
		if ls := s.idx.GetLatestState(); ls != nil {
			lastAt = ls.AtBlock
			prevSeqno = &lastAt
		}

		diff := int64(keyBlockSeqno) - int64(lastAt)

		if !mandatory && diff < int64(s.stateEveryBlocksNum) {
			log.Debug().
				Int64("left", int64(s.stateEveryBlocksNum)-diff).
				Msg("blocks till next state serialization")
			continue
		}

		if skipPrev {
			prevSeqno = nil
		}

		ctx, cancel := context.WithTimeout(s.closerCtx, 14*24*time.Hour)
		path, err := s.gen.Generate(ctx, keyBlockSeqno, prevSeqno)
		cancel()

		if err != nil {
			log.Error().Err(err).Uint32("block", keyBlockSeqno).Msg("failed to generate state")
			if !skipPrev {
				skipPrev = true
				log.Info().Uint32("block", keyBlockSeqno).Msg("will retry without prev state to not get memory kill")
			}
			wait = 5 * time.Second
			continue
		}
		skipPrev = false

		ctx, cancel = context.WithTimeout(s.closerCtx, 3*time.Hour)
		bagId, err := s.storage.CreateBag(ctx, path, fmt.Sprintf("%s State at %d block", s.bagPrefix, keyBlockSeqno), nil)
		cancel()

		if err != nil {
			log.Error().Err(err).Uint32("block", keyBlockSeqno).Msg("failed to create bag for state")
			wait = 5 * time.Second
			continue
		}

		s.idx.AddState(index.State{
			AtBlock: keyBlockSeqno,
			Bag:     hex.EncodeToString(bagId),
		})
		if err = s.idx.Save(); err != nil {
			log.Error().Err(err).Uint32("block", keyBlockSeqno).Msg("failed to save index")
			wait = 5 * time.Second
			continue
		}

		if mandatory {
			wait = 0
		}
	}
}

func (s *Service) Stop() {
	s.closer()
}

func (s *Service) latestNodeSeqnoFinished() (uint32, error) {
	stats, err := s.node.GetStats(s.closerCtx)
	if err != nil {
		return 0, fmt.Errorf("failed to get stats: %w", err)
	}

	seqno, err := strconv.ParseUint(stats["shardclientmasterchainseqno"], 10, 32)
	if err != nil || seqno == 0 {
		if seqno == 0 {
			err = fmt.Errorf("seqno is 0")
		}
		return 0, fmt.Errorf("failed to parse seqno: %w", err)
	}
	seqno -= 1

	pack, err := s.fsWatcher.FindPack(uint32(seqno))
	if err != nil {
		return 0, fmt.Errorf("failed to find pack for seqno: %w", err)
	}

	return pack.Seqno - 1, nil
}

func (s *Service) latestKeyBlock() (uint32, error) {
	stats, err := s.node.GetStats(s.closerCtx)
	if err != nil {
		return 0, fmt.Errorf("failed to get stats: %w", err)
	}

	blk := stats["keymasterchainblock"]

	// Extracting the third uint32 value from `blk` field
	start := strings.Index(blk, "(")
	end := strings.Index(blk, ")")
	if start == -1 || end == -1 || start >= end {
		return 0, fmt.Errorf("invalid block format: %s", blk)
	}

	// Splitting values inside the parentheses
	parts := strings.Split(blk[start+1:end], ",")
	if len(parts) < 3 {
		return 0, fmt.Errorf("not enough parts in block format: %s", blk)
	}

	// Parsing the third value as uint32
	seqno, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse third value: %w", err)
	}

	return uint32(seqno), nil
}

func (s *Service) latestKeyBlockForNextState() (uint32, bool, error) {
	have := s.idx.GetLatestState()

	for _, initialState := range s.initialStates {
		if have == nil || have.AtBlock < initialState {
			return initialState, true, nil
		}
	}

	blk, err := s.latestKeyBlock()
	if err != nil {
		return 0, false, err
	}

	return blk, false, nil
}

func (s *Service) SplitBlockPacks(dirToPut string) error {
	for i, block := range s.idx.Blocks {
		log.Info().Uint32("from", block.FromPack).Uint32("to", block.ToPack).Msg("checking pack content")
		bag, _ := hex.DecodeString(block.Bag)
		details, err := s.storage.GetBag(context.Background(), bag)
		if err != nil {
			return fmt.Errorf("get bag %s: %w", block.Bag, err)
		}

		bagPath := filepath.Clean(filepath.Join(details.Path, details.DirName))
		log.Info().Hex("bag", bag).Str("path", bagPath).Msg("checking bag")

		if bagPath == filepath.Clean(dirToPut) {
			log.Info().Hex("bag", bag).Msg("already processed bag, skipping")
			continue
		}

		var hasChanges bool
		var filesForBag []string
		var writers []func() error
		for _, file := range details.Files {
			spl := strings.Split(file.Name, ".")
			if len(spl) < 3 {
				return fmt.Errorf("invalid file name: %s", file.Name)
			}

			path := filepath.Join(details.Path, details.DirName, file.Name)

			if len(spl) > 3 {
				// already split
				nm := filepath.Join(dirToPut, file.Name)

				writers = append(writers, func() error {
					if err = os.MkdirAll(filepath.Dir(nm), 0755); err != nil {
						return fmt.Errorf("create dir %s: %w", filepath.Dir(nm), err)
					}

					log.Info().Str("to", nm).Str("from", path).Msg("copying package")

					fl, err := os.Open(path)
					if err != nil {
						return fmt.Errorf("open file %s: %w", path, err)
					}
					defer fl.Close()

					if _, err = fl.Seek(0, io.SeekStart); err != nil {
						return fmt.Errorf("seek file %s: %w", path, err)
					}

					dest, err := os.Create(nm)
					if err != nil {
						return fmt.Errorf("create destination file %s: %w", nm, err)
					}
					defer dest.Close()

					if _, err = io.Copy(dest, fl); err != nil {
						return fmt.Errorf("copy file to %s: %w", nm, err)
					}
					return nil
				})

				filesForBag = append(filesForBag, nm)
				continue
			}

			// log.Info().Str("file", path).Msg("checking pack")

			fl, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("open file %s: %w", path, err)
			}

			id, err := strconv.ParseUint(spl[1], 10, 32)
			if err != nil {
				fl.Close()
				return fmt.Errorf("invalid file id %s: %w", spl[1], err)
			}

			pack, err := packsplit.ReadPackage(fl)
			fl.Close()
			if err != nil {
				return fmt.Errorf("read file %s: %w", path, err)
			}

			fls, err := packsplit.SplitByFiles(int(id), pack)
			if err != nil {
				return fmt.Errorf("split file %s: %w", path, err)
			}

			if len(fls) > 1 {
				hasChanges = true
			}

			for nm, f := range fls {
				nm = filepath.Join(dirToPut, filepath.Dir(file.Name), nm)
				filesForBag = append(filesForBag, nm)

				entries := f
				writers = append(writers, func() error {
					if err = os.MkdirAll(filepath.Dir(nm), 0755); err != nil {
						return fmt.Errorf("create dir %s: %w", filepath.Dir(nm), err)
					}

					log.Info().Str("file", nm).Msg("writing package")

					nf, err := os.Create(nm)
					if err != nil {
						return fmt.Errorf("create file %s: %w", nm, err)
					}
					defer nf.Close()

					if err = packsplit.WritePackage(nf, entries); err != nil {
						return fmt.Errorf("write new file %s: %w", nm, err)
					}
					return nil
				})
			}
		}

		if !hasChanges {
			log.Info().Hex("bag", bag).Msg("no changes, skipping")
			continue
		}

		log.Info().Hex("bag", bag).Msg("changes found, replacing")

		for x, writer := range writers {
			if err = writer(); err != nil {
				return fmt.Errorf("failed writer %s: %w", filesForBag[x], err)
			}
		}
		sort.Strings(filesForBag)

		dir := filepath.Dir(filepath.Dir(filesForBag[0]))
		log.Info().Str("root", dir).Int("files", len(filesForBag)).Str("first_path", filesForBag[0]).Msg("new bag info")

		newId, err := s.storage.CreateBag(context.Background(), dir, details.Bag.Description, filesForBag)
		if err != nil {
			return fmt.Errorf("create bag %s: %w", details.Bag.Description, err)
		}

		if bytes.Equal(bag, newId) {
			log.Info().Hex("bag", bag).Msg("bag not changed, skipping")
			continue
		}

		s.idx.Blocks[i].Bag = hex.EncodeToString(newId)

		if err = s.idx.Save(); err != nil {
			return fmt.Errorf("save idx %s: %w", details.Bag.Description, err)
		}
		log.Info().Hex("from", bag).Hex("to", newId).Int("files_before", len(details.Files)).Int("files_after", len(filesForBag)).Msg("index bag replaced")

		if err = s.storage.RemoveBag(context.Background(), bag, false); err != nil {
			return fmt.Errorf("remove bag %s: %w", hex.EncodeToString(bag), err)
		}
		log.Info().Hex("bag", bag).Msg("old bag removed")
	}

	return nil
}

func (s *Service) RepackBlockBags() error {
	for i, block := range s.idx.Blocks {
		log.Info().Uint32("from", block.FromPack).Uint32("to", block.ToPack).Msg("checking bag content")
		bag, _ := hex.DecodeString(block.Bag)
		details, err := s.storage.GetBag(context.Background(), bag)
		if err != nil {
			return fmt.Errorf("get bag %s: %w", block.Bag, err)
		}

		var filesRaw []string
		for _, file := range details.Files {
			filesRaw = append(filesRaw, filepath.Join(details.Path, details.DirName, file.Name))
		}

		files, err := sortFiles(filesRaw)
		if err != nil {
			return fmt.Errorf("sort files: %w", err)
		}

		// Compare the files list after sorting to ensure no changes
		if slices.Equal(filesRaw, files) {
			log.Info().Hex("bag", bag).Msg("files are equal after sorting, skipping")
			continue
		}

		newId, err := s.storage.CreateBag(context.Background(), filepath.Join(details.Path, details.DirName), details.Bag.Description, files)
		if err != nil {
			return fmt.Errorf("create bag %s: %w", details.Bag.Description, err)
		}

		if bytes.Equal(bag, newId) {
			log.Info().Hex("bag", bag).Msg("bag not changed, skipping")
			continue
		}

		s.idx.Blocks[i].Bag = hex.EncodeToString(newId)

		if err = s.idx.Save(); err != nil {
			return fmt.Errorf("save idx %s: %w", details.Bag.Description, err)
		}
		log.Info().Hex("from", bag).Hex("to", newId).Int("files_before", len(details.Files)).Int("files_after", len(files)).Msg("index bag replaced")

		if err = s.storage.RemoveBag(context.Background(), bag, false); err != nil {
			return fmt.Errorf("remove bag %s: %w", hex.EncodeToString(bag), err)
		}
		log.Info().Hex("bag", bag).Msg("old bag removed")
	}

	return nil
}
