package storage

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"net/http"
)

type Client struct {
	base        string
	client      http.Client
	credentials *Credentials

	logger zerolog.Logger
}

type Credentials struct {
	Login    string
	Password string
}

var ErrNotFound = errors.New("not found")

func NewClient(base string, credentials *Credentials, logger zerolog.Logger) *Client {
	return &Client{
		base:        base,
		client:      http.Client{},
		credentials: credentials,
		logger:      logger,
	}
}

func (c *Client) GetBag(ctx context.Context, bagId []byte) (*BagDetailed, error) {
	var res BagDetailed
	if err := c.doRequest(ctx, "GET", "/api/v1/details?bag_id="+hex.EncodeToString(bagId), nil, &res); err != nil {
		return nil, fmt.Errorf("failed to do request: %w", err)
	}

	return &res, nil
}

func (c *Client) GetPieceProof(ctx context.Context, bagId []byte, piece uint64) ([]byte, error) {
	var res ProofResponse
	if err := c.doRequest(ctx, "GET", "/api/v1/piece/proof?bag_id="+hex.EncodeToString(bagId)+"&piece="+fmt.Sprint(piece), nil, &res); err != nil {
		return nil, fmt.Errorf("failed to do request: %w", err)
	}
	return res.Proof, nil
}

func (c *Client) CreateBag(ctx context.Context, path, description string, only []string) ([]byte, error) {
	c.logger.Info().Str("path", path).Str("description", description).Msg("creating bag")

	type request struct {
		Path          string   `json:"path"`
		Description   string   `json:"description"`
		KeepOnlyPaths []string `json:"keep_only_paths"`
	}

	type response struct {
		BagID string `json:"bag_id"`
	}

	var res response
	if err := c.doRequest(ctx, "POST", "/api/v1/create", request{
		Path:          path,
		Description:   description,
		KeepOnlyPaths: only,
	}, &res); err != nil {
		return nil, fmt.Errorf("failed to do request: %w", err)
	}

	bagId, err := hex.DecodeString(res.BagID)
	if err != nil {
		return nil, fmt.Errorf("failed to decode bag id: %w", err)
	}

	c.logger.Info().Str("path", path).Hex("id", bagId).Str("description", description).Msg("bag created")

	return bagId, nil
}

func (c *Client) ListBags(ctx context.Context) ([]Bag, error) {
	type response struct {
		Bags []Bag `json:"bags"`
	}

	var res response
	if err := c.doRequest(ctx, "GET", "/api/v1/list", nil, &res); err != nil {
		return nil, fmt.Errorf("failed to do request: %w", err)
	}

	return res.Bags, nil
}

func (c *Client) RemoveBag(ctx context.Context, bagId []byte, withFiles bool) error {
	type request struct {
		BagID     string `json:"bag_id"`
		WithFiles bool   `json:"with_files"`
	}

	var res Result
	if err := c.doRequest(ctx, "POST", "/api/v1/remove", request{
		BagID:     hex.EncodeToString(bagId),
		WithFiles: withFiles,
	}, &res); err != nil {
		return fmt.Errorf("failed to do request: %w", err)
	}

	if !res.Ok {
		return fmt.Errorf("error in response: %s", res.Error)
	}
	return nil
}

func (c *Client) doRequest(ctx context.Context, method, url string, req, resp any) error {
	buf := &bytes.Buffer{}
	if req != nil {
		if err := json.NewEncoder(buf).Encode(req); err != nil {
			return fmt.Errorf("failed to encode request data: %w", err)
		}
	}

	r, err := http.NewRequestWithContext(ctx, method, c.base+url, buf)
	if err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	}
	if c.credentials != nil {
		r.SetBasicAuth(c.credentials.Login, c.credentials.Password)
	}

	res, err := c.client.Do(r)
	if err != nil {
		return fmt.Errorf("failed to make request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return ErrNotFound
	}

	if res.StatusCode != 200 {
		var e Result
		if err = json.NewDecoder(res.Body).Decode(&e); err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}
		return fmt.Errorf("status code is %d, error: %s", res.StatusCode, e.Error)
	}

	if err = json.NewDecoder(res.Body).Decode(resp); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}
	return nil
}
