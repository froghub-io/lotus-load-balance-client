package cliutil

import (
	"net/http"
	"os"
	"strings"

	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-jsonrpc"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	cutil "github.com/filecoin-project/lotus/cli/util"
	"github.com/froghub-io/lotus-load-balance-client/client"
)

func GetLoadBalanceFullNodeAPIV1(ctx *cli.Context) (v1api.FullNode, jsonrpc.ClientCloser, error) {
	if tn, ok := ctx.App.Metadata["testnode-full"]; ok {
		return tn.(v1api.FullNode), func() {}, nil
	}

	var apiInfos string
	if apiInfos = os.Getenv("FULLNODE_API_INFOS"); apiInfos == "" {
		return nil, nil, xerrors.Errorf("Environment variable FULLNODE_API_INFOS not found")
	}

	raws := strings.Split(apiInfos, ",")
	addrs := make([]string, 0)
	headers := make([]http.Header, 0)
	for i, ai := range raws {
		info := cutil.ParseApiInfo(ai)
		if addr, err := info.DialArgs("v1"); err != nil {
			return nil, nil, xerrors.Errorf("invalid node info: index %d", i)
		} else {
			addrs = append(addrs, addr)
		}
		headers = append(headers, info.AuthHeader())
	}

	v1API, closer, err := client.NewLoadBalanceFullNodeRPCV1(ctx.Context, addrs, headers)
	if err != nil {
		return nil, nil, err
	}

	v, err := v1API.Version(ctx.Context)
	if err != nil {
		return nil, nil, err
	}
	if !v.APIVersion.EqMajorMinor(api.FullAPIVersion1) {
		return nil, nil, xerrors.Errorf("Remote API version didn't match (expected %s, remote %s)", api.FullAPIVersion1, v.APIVersion)
	}

	return v1API, closer, nil
}
