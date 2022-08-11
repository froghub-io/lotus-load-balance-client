# Lotus Load Balance Client

This is a soft load balancing client (as a library) with the heaviest chain selector. By dynamically selecting the heaviest chain node, 
it reduces the impact of network factors on chain synchronization and avoids the problem of single point of failure.

## Notes on use in lotus

Since this library depends on the related packages in lotus (eg. api package), when using in lotus, 
in order to avoid circular dependencies, it is recommended to copy the source code to the following 
lotus directory/file with the same name:

``` go
//lotus-load-balance-client/client/
lotus/api/client/
//lotus-load-balance-client/cliutil/api.go
lotus/cli/util/api.go
```

Finally, Simply replace the relevant dependency packages to use


## Usage

The environment variable FULLNODE_API_INFOS needs to be set and its value is an array of FULLNODE_API_INFO value(separated by commas, 
such as xxx,xxx,xxx)

The following is an example of use:
```
package xxx

import (
	"context"
	"sync"
	"testing"

	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"github.com/froghub-io/lotus-load-balance-client/cliutil"
)

func TestGetLoadBalanceFullNodeAPIV1(t *testing.T) {
	logging.SetLogLevel("*", "INFO")
	ctx := context.Background()

	c := cli.NewContext(cli.NewApp(), nil, nil)
	c.Context = ctx

	fullNode, closer, err := cliutil.GetLoadBalanceFullNodeAPIV1(c)
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
```
Note: If a signed message is involved, you need to import wallets for all chain nodes so that the message can be signed normally

## Features in the plan

* Allow independent configuration of the list of chain nodes used to sign messages, to avoid all nodes importing the wallet
* Allows to run as a gateway service and Wallet unified management in the gateway.

## Reference

This library refers to the chain selection design of [chain-co](https://github.com/dtynn/chain-co) and makes adjustments and 
optimizations.
