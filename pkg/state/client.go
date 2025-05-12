package state

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Generator handles serialization of states by invoking an external utility.
type Generator struct {
	util    string
	dbPath  string
	outPath string
	api     ton.APIClientWrapped

	logger zerolog.Logger
}

// NewGenerator creates a new Generator with the given utility path, database directory, and output directory.
func NewGenerator(api ton.APIClientWrapped, util, dbDir, outDir string, logger zerolog.Logger) *Generator {
	return &Generator{
		api:     api,
		util:    util,
		dbPath:  dbDir,
		outPath: outDir,
		logger:  logger,
	}
}

// Generate processes a single state by sequence number and data string,
// invoking the external utility with the given context, and returning the output folder path or an error.
// ctx controls cancellation or timeout of the external command.
// seqno is the sequence number of the state.
// prevSeqno is an optional pointer to the previous sequence number; if non-nil,
// the -p flag will be added using the previous output folder.
func (g *Generator) Generate(ctx context.Context, seqno uint32, prevSeqno *uint32) (string, error) {
	g.logger.Info().Uint32("seqno", seqno).Msg("serializing state")

	var err error
	var blk *ton.BlockIDExt
	if seqno == 0 {
		blk, err = g.api.LookupBlock(ctx, -1, int64(-0x8000000000000000), 1)
		if err != nil {
			return "", fmt.Errorf("failed to lookup first block: %w", err)
		}

		b, err := g.api.GetBlockData(ctx, blk)
		if err != nil {
			return "", fmt.Errorf("failed to get block data: %w", err)
		}

		blk.SeqNo = 0
		blk.RootHash = b.BlockInfo.PrevRef.Prev1.RootHash
		blk.FileHash = b.BlockInfo.PrevRef.Prev1.FileHash
	} else {
		blk, err = g.api.LookupBlock(ctx, -1, int64(-0x8000000000000000), seqno)
		if err != nil {
			return "", fmt.Errorf("failed to lookup block: %w", err)
		}
	}
	stateData := fmt.Sprintf("(-1,8000000000000000,%d):%s:%s", seqno, strings.ToUpper(hex.EncodeToString(blk.RootHash)), strings.ToUpper(hex.EncodeToString(blk.FileHash)))

	// Prepare output path and folder name
	folderName := fmt.Sprintf("state-%09d", seqno)
	outPath := filepath.Join(g.outPath, folderName) + string(os.PathSeparator)

	// Build command arguments
	args := []string{"-o", outPath, "-D", g.dbPath, "-m", stateData}
	if prevSeqno != nil {
		prevFolder := fmt.Sprintf("state-%09d", *prevSeqno)
		prevPath := filepath.Join(g.outPath, prevFolder) + string(os.PathSeparator)
		args = append(args, "-p", prevPath)
	}

	// Execute utility command with context for cancellation or timeout
	cmd := exec.CommandContext(ctx, g.util, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("error executing util for seqno=%d: %w", seqno, err)
	}

	return outPath, nil
}
