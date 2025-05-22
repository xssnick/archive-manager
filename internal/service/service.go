package service

import (
	"archive-manager/pkg/control"
	"archive-manager/pkg/fs"
	"archive-manager/pkg/index"
	"archive-manager/pkg/state"
	"archive-manager/pkg/storage"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"path/filepath"
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

					sort.Strings(filesForBag)

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

		ctx, cancel := context.WithTimeout(s.closerCtx, 7*24*time.Hour)
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
