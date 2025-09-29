package main

import (
	"context"
	"encoding/hex"
	"flag"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/xssnick/archive-manager/pkg/index"
	"github.com/xssnick/archive-manager/pkg/storage"
	"os"
	"time"
)

func main() {
	storageBase := flag.String("storage-url", "", "URL of the storage base")
	storageLogin := flag.String("storage-login", "", "Login for the storage")
	storagePassword := flag.String("storage-password", "", "Password for the storage")
	indexPath := flag.String("index-path", "", "Index path")

	flag.Parse()
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	stg := storage.NewClient(*storageBase, &storage.Credentials{
		Login:    *storageLogin,
		Password: *storagePassword,
	}, log.With().Str("source", "storage").Logger())

	idx, err := index.Load(*indexPath, nil, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load index")
	}

	ch := make(chan bool, 8)
	for _, block := range idx.Blocks {
		ch <- true
		go func() {
			defer func() { <-ch }()

			bag, err := hex.DecodeString(block.Bag)
			if err != nil {
				log.Fatal().Err(err).Str("bag", block.Bag).Msg("failed to decode bag")
				return
			}

			log.Info().Str("bag", block.Bag).Msg("downloading bag")
			if err = stg.StartDownload(context.Background(), bag, true); err != nil {
				log.Fatal().Err(err).Str("bag", block.Bag).Msg("failed to start download")
				return
			}

			for {
				details, err := stg.GetBag(context.Background(), bag)
				if err != nil {
					log.Fatal().Err(err).Str("bag", block.Bag).Msg("failed to get bag")
					return
				}

				if details.Completed {
					log.Info().Str("bag", block.Bag).Msg("bag downloaded")
					break
				}

				log.Info().Str("bag", block.Bag).Float64("progress", float64(details.Downloaded)/float64(details.Size)).Uint64("speed_kb", details.DownloadSpeed>>10).Msg("progress")
				time.Sleep(1000 * time.Millisecond)
			}
		}()
	}
}
