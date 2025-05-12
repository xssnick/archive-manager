package index

import (
	"encoding/json"
	"os"
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

	path string
}

func (idx *Index) AddState(state State) {
	idx.States = append(idx.States, state)
}

func (idx *Index) AddBlockSet(bs BlockSet) {
	idx.Blocks = append(idx.Blocks, bs)
}

func (idx *Index) Save() error {
	data, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(idx.path, data, 0644)
}

func Load(path string) (*Index, error) {
	var idx Index
	idx.path = path

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
