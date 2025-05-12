package index

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/ton/dns"
	"github.com/xssnick/tonutils-go/ton/wallet"
)

type Updater struct {
	domain *dns.Domain
	wl     *wallet.Wallet

	log zerolog.Logger
}

func InitUpdater(ctx context.Context, api ton.APIClientWrapped, domain string, key ed25519.PrivateKey, logger zerolog.Logger) (*Updater, error) {
	w, err := wallet.FromPrivateKey(api, key, wallet.V3R2)
	if err != nil {
		return nil, fmt.Errorf("failed to init wallet: %w", err)
	}

	root, err := dns.GetRootContractAddr(ctx, api)
	if err != nil {
		return nil, fmt.Errorf("failed to get dns root contract address: %w", err)
	}

	dom, err := dns.NewDNSClient(api, root).Resolve(ctx, domain)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve domain: %w", err)
	}

	data, err := dom.GetNFTData(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get domain data: %w", err)
	}

	if !data.OwnerAddress.Equals(w.WalletAddress()) {
		return nil, fmt.Errorf("domain is not owned by this wallet")
	}

	return &Updater{
		domain: dom,
		wl:     w,
		log:    logger,
	}, nil
}

func (u *Updater) UpdateIndexRecord(ctx context.Context, bag []byte) error {
	if len(bag) != 32 {
		return fmt.Errorf("invalid bag id length")
	}

	payload := u.domain.BuildSetSiteRecordPayload(bag, true)

	tx, _, err := u.wl.SendWaitTransaction(ctx, wallet.SimpleMessage(u.domain.GetNFTAddress(), tlb.MustFromTON("0.05"), payload))
	if err != nil {
		return fmt.Errorf("failed to send transaction: %w", err)
	}

	u.log.Info().Hex("tx", tx.Hash).Msg("transaction confirmed, index bag has been updated")
	return nil
}
