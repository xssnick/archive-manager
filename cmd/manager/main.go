package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"flag"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/xssnick/archive-manager/internal/service"
	"github.com/xssnick/archive-manager/pkg/control"
	"github.com/xssnick/archive-manager/pkg/fs"
	"github.com/xssnick/archive-manager/pkg/index"
	"github.com/xssnick/archive-manager/pkg/state"
	"github.com/xssnick/archive-manager/pkg/storage"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/ton"
	"os"
	"os/signal"
)

type Config struct {
	ControlAddr                  string   `json:"control_addr"`
	ControlServerKey             string   `json:"control_server_key"`
	ControlClientKey             string   `json:"control_client_key"`
	LiteServerAddr               string   `json:"liteserver_addr"`
	LiteServerKey                string   `json:"liteserver_key"`
	StorageBase                  string   `json:"storage_url"`
	StorageLogin                 string   `json:"storage_login"`
	StoragePassword              string   `json:"storage_password"`
	WatcherRoot                  string   `json:"watcher_root"`
	IndexPath                    string   `json:"index_path"`
	SerializePath                string   `json:"serialize_path"`
	CelldbPath                   string   `json:"celldb_path"`
	StatesPath                   string   `json:"states_path"`
	ServiceName                  string   `json:"bag_prefix"`
	ServiceCache                 uint64   `json:"bag_size"`
	WatchBlocks                  bool     `json:"watch_blocks"`
	WatchStates                  bool     `json:"watch_states"`
	SerializeStateEveryBlocksNum uint32   `json:"serialize_state_every_blocks_num"`
	InitialStates                []uint32 `json:"initial_states"`
}

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	log.Info().Msg("loading configuration...")

	configFileName := flag.String("config", "config.json", "path to configuration file")
	verbosity := flag.String("verbosity", "info", "set the logging verbosity level (debug, info, warn, error, fatal)")
	splitBlocks := flag.String("split-blocks", "", "split block packs to master and shards, path to put splited")
	repackBlockBags := flag.Bool("repack-block-bags", false, "repack block bags to optimize download size")

	flag.Parse()

	level, err := zerolog.ParseLevel(*verbosity)
	if err != nil {
		log.Fatal().Err(err).Msg("invalid verbosity level")
	}
	zerolog.SetGlobalLevel(level)

	configFile, err := os.Open(*configFileName)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open config file")
	}
	defer configFile.Close()

	var cfg Config
	if err := json.NewDecoder(configFile).Decode(&cfg); err != nil {
		log.Fatal().Err(err).Msg("failed to decode config file")
	}

	if !cfg.WatchBlocks && !cfg.WatchStates {
		log.Fatal().Msg("watch-blocks or watch-states should be enabled")
	}

	if len(cfg.InitialStates) == 0 && cfg.WatchStates {
		log.Fatal().Msg("initial-states should be specified if watch-states is enabled")
	}
	if cfg.WatchStates && cfg.SerializeStateEveryBlocksNum == 0 {
		log.Fatal().Msg("serialize-state-every-blocks-num should be specified if watch-states is enabled")
	}

	key, err := base64.StdEncoding.DecodeString(cfg.ControlClientKey)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to decode control key")
	}

	cli, err := control.NewClient(context.Background(), cfg.ControlAddr, ed25519.NewKeyFromSeed(key), cfg.ControlServerKey)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect")
	}

	stg := storage.NewClient(cfg.StorageBase, &storage.Credentials{
		Login:    cfg.StorageLogin,
		Password: cfg.StoragePassword,
	}, log.With().Str("source", "storage").Logger())

	idx, err := index.Load(cfg.IndexPath)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load index")
	}

	if *repackBlockBags {
		svc := service.NewService(cli, nil, idx, nil, stg, cfg.ServiceName, cfg.ServiceCache, cfg.SerializeStateEveryBlocksNum, cfg.InitialStates)
		if err = svc.RepackBlockBags(); err != nil {
			log.Fatal().Err(err).Msg("failed to repack block bags")
		}
		log.Info().Msg("repack block bags completed")
		return
	}

	if *splitBlocks != "" {
		svc := service.NewService(cli, nil, idx, nil, stg, cfg.ServiceName, cfg.ServiceCache, cfg.SerializeStateEveryBlocksNum, cfg.InitialStates)
		if err = svc.SplitBlockPacks(*splitBlocks); err != nil {
			log.Fatal().Err(err).Msg("failed to split block packs")
		}
		log.Info().Msg("split block packs completed")
		return
	}

	client := liteclient.NewConnectionPool()

	log.Info().Msg("connecting to liteserver")

	err = client.AddConnection(context.Background(), cfg.LiteServerAddr, cfg.LiteServerKey)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to liteserver")
	}
	api := ton.NewAPIClient(client, ton.ProofCheckPolicyFast).WithRetry()

	if _, err = api.GetMasterchainInfo(context.Background()); err != nil {
		log.Fatal().Err(err).Msg("failed to get masterchain info")
	}

	var w *fs.Watcher
	if cfg.WatchBlocks {
		w = fs.NewWatcher(cfg.WatcherRoot, log.With().Str("source", "watcher").Logger())
		go w.Watch()
	}

	var gen *state.Generator
	if cfg.WatchStates {
		gen = state.NewGenerator(api, cfg.SerializePath, cfg.CelldbPath, cfg.StatesPath, log.With().Str("source", "states").Logger())
	}

	log.Info().Msg("starting service")

	svc := service.NewService(cli, w, idx, gen, stg, cfg.ServiceName, cfg.ServiceCache, cfg.SerializeStateEveryBlocksNum, cfg.InitialStates)

	if cfg.WatchBlocks {
		go svc.StartBlocks()
	}
	if cfg.WatchStates {
		go svc.StartStates()
	}

	// Wait for interrupt signal (Ctrl+C) to gracefully shut down
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	log.Info().Msg("shutting down...")
}
