package control

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
)

func init() {
	tl.Register(GetStats{}, "engine.validator.getStats = engine.validator.Stats")

	tl.Register(OneStat{}, "engine.validator.oneStat key:string value:string = engine.validator.OneStat")
	tl.Register(Stats{}, "engine.validator.stats stats:(vector engine.validator.oneStat) = engine.validator.Stats")

	tl.Register(Query{}, "engine.validator.controlQuery data:bytes = Object")
	tl.Register(QueryError{}, "engine.validator.controlQueryError code:int message:string = engine.validator.ControlQueryError")
}

type Query struct {
	Data any `tl:"bytes struct boxed"`
}

type QueryError struct {
	Code    int32  `tl:"int"`
	Message string `tl:"string"`
}

type GetStats struct{}

type OneStat struct {
	Key   string `tl:"string"`
	Value string `tl:"string"`
}

type Stats struct {
	Stats []*OneStat `tl:"vector struct"`
}

type Client struct {
	conn *liteclient.ConnectionPool
}

func NewClient(ctx context.Context, addr string, authKey ed25519.PrivateKey, serverKey string) (*Client, error) {
	pool := liteclient.NewConnectionPoolWithAuth(authKey)

	err := pool.AddConnection(ctx, addr, serverKey, authKey)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn: pool,
	}, nil
}

func (c *Client) GetStats(ctx context.Context) (map[string]string, error) {
	var res any
	if err := c.conn.QueryADNL(ctx, Query{GetStats{}}, &res); err != nil {
		return nil, err
	}

	switch v := res.(type) {
	case QueryError:
		return nil, fmt.Errorf("query error: %s", v.Message)
	case Stats:
		statsMap := make(map[string]string)
		for _, stat := range v.Stats {
			statsMap[stat.Key] = stat.Value
		}
		return statsMap, nil
	}
	return nil, fmt.Errorf("unknown response type: %T", res)
}
