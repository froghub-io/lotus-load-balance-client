package cliutil

import (
	"context"
	"sync"
	"testing"

	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

func TestGetLoadBalanceFullNodeAPIV1(t *testing.T) {
	logging.SetLogLevel("*", "INFO")
	ctx := context.Background()

	c := cli.NewContext(cli.NewApp(), nil, nil)
	c.Context = ctx

	fullNode, closer, err := GetLoadBalanceFullNodeAPIV1(c)
	if err != nil {
		t.Log(err)
		return
	}
	defer closer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			ts, err := fullNode.ChainHead(ctx)
			if err != nil {
				t.Log(err)
			} else {
				t.Log("ts:", ts)
			}
			<-time.After(6 * time.Second)
		}
	}()
	wg.Wait()
}
