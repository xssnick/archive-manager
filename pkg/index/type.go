package index

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/xssnick/archive-manager/pkg/storage"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type BlockSet struct {
	FromPack uint32 `json:"from"`
	ToPack   uint32 `json:"to"`
	Bag      string `json:"bag"`
}

type State struct {
	AtBlock uint32 `json:"at_block"`
	Bag     string `json:"bag"`
}

type Index struct {
	States []State    `json:"states"`
	Blocks []BlockSet `json:"blocks"`

	path    string
	updater *Updater
	storage *storage.Client

	mx sync.RWMutex
}

func (idx *Index) AddState(state State) {
	idx.mx.Lock()
	defer idx.mx.Unlock()

	idx.States = append(idx.States, state)
}

func (idx *Index) AddBlockSet(bs BlockSet) {
	idx.mx.Lock()
	defer idx.mx.Unlock()

	idx.Blocks = append(idx.Blocks, bs)
}

func (idx *Index) Save(ctx context.Context) error {
	idx.mx.Lock()
	defer idx.mx.Unlock()

	data, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}

	if err = os.WriteFile(idx.path, data, 0644); err != nil {
		return err
	}

	if idx.updater != nil && idx.storage != nil {
		dir := filepath.Dir(idx.path)

		path := filepath.Join(dir, "index_bags", fmt.Sprintf("%d_%d", time.Now().Unix(), crc32.ChecksumIEEE(data)), "index", "index.json")
		if err = os.WriteFile(path, data, 0644); err != nil {
			return err
		}

		bagId, err := idx.storage.CreateBag(ctx, path, fmt.Sprintf("TON Archive index at %s", time.Now().Format(time.RFC822)), nil)
		if err != nil {
			return err
		}

		if err = idx.updater.UpdateIndexRecord(ctx, bagId); err != nil {
			return err
		}
	}

	return nil
}

func Load(path string, updater *Updater, stg *storage.Client) (*Index, error) {
	var idx Index
	idx.path = path
	idx.updater = updater
	idx.storage = stg

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			idx.States = []State{}
			idx.Blocks = []BlockSet{}
			return &idx, nil
		}
		return nil, err
	}
	return &idx, json.Unmarshal(data, &idx)
}

func (idx *Index) GetLatestState() *State {
	idx.mx.RLock()
	defer idx.mx.RUnlock()

	if len(idx.States) == 0 {
		return nil
	}

	latest := idx.States[0]
	for _, s := range idx.States[1:] {
		if s.AtBlock > latest.AtBlock {
			latest = s
		}
	}
	return &latest
}

func (idx *Index) GetLatestBlock() BlockSet {
	idx.mx.RLock()
	defer idx.mx.RUnlock()

	if len(idx.Blocks) == 0 {
		return BlockSet{}
	}
	
	latest := idx.Blocks[0]
	for _, b := range idx.Blocks[1:] {
		if b.ToPack > latest.ToPack {
			latest = b
		}
	}
	return latest
}
