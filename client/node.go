package client

import (
	"context"
	"sync"
	"time"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
)

// DefaultNodeOption returns default options
func DefaultNodeOption() NodeOption {
	return NodeOption{
		ReListenMinInterval: 4 * time.Second,
		ReListenMaxInterval: 32 * time.Second,
		APITimeout:          10 * time.Second,
	}
}

// NodeOption is for node configuration
type NodeOption struct {
	ReListenMinInterval time.Duration
	ReListenMaxInterval time.Duration

	APITimeout time.Duration
}

type Node struct {
	opt              NodeOption
	address          string
	relistenInterval time.Duration

	upstream struct {
		full   v1api.FullNode
		closer jsonrpc.ClientCloser
	}

	headCh         chan *headCandidate
	loseHeartCh    chan string
	receiveHeartCh chan struct {
		addr    string
		version api.Version
	}

	loadCacheBlockHeader func(c cid.Cid) (*types.BlockHeader, bool)
	addCacheBlockHeaders func(changes []*api.HeadChange)
}

func (n *Node) Start(c context.Context) context.CancelFunc {
	ctx, cf := context.WithCancel(c)
	wcf := func() {
		n.upstream.closer()
		cf()
	}
	go func() {
		log.Infof("start head change loop, node %s", n.address)
		defer log.Infof("stop head change loop, node %s", n.address)
		for {
			ch, err := n.reListen(ctx)
			if err != nil {
				if err != context.Canceled && err != context.DeadlineExceeded {
					log.Errorf("node %s failed to listen head change: %s", n.address, err)
				}
				return
			}

			cctx, abort := context.WithCancel(ctx)
		CHANGES_LOOP:
			for {
				select {
				case <-ctx.Done():
					abort()
					return
				case changes, ok := <-ch:
					if !ok {
						break CHANGES_LOOP
					}
					go n.applyChanges(cctx, changes)
				}
			}
			abort()
		}
	}()

	return wcf
}

func (n *Node) reListen(ctx context.Context) (<-chan []*api.HeadChange, error) {
	for {

		_, err := n.CheckHealth(ctx)
		if err == nil {
			c, err := n.upstream.full.ChainNotify(ctx)
			if err == nil {
				return c, nil
			} else {
				log.Errorf("node %s Call ChainNotify error: %s, with recall in %s", n.address, err, n.relistenInterval)
			}
		} else {
			log.Errorf("node %s CheckHealth error: %s, with recall in %s", n.address, err, n.relistenInterval)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(n.relistenInterval):
			n.relistenInterval *= 2
			if n.relistenInterval > n.opt.ReListenMaxInterval {
				n.relistenInterval = n.opt.ReListenMaxInterval
			}
			continue
		}
	}
}

func (n *Node) CheckHealth(ctx context.Context) (api.Version, error) {

	apiVersion, err := n.upstream.full.Version(ctx)
	if err != nil {
		n.loseHeartCh <- n.address
		return 0, err
	}

	n.receiveHeartCh <- struct {
		addr    string
		version api.Version
	}{
		addr:    n.address,
		version: apiVersion.APIVersion,
	}
	return apiVersion.APIVersion, nil
}

func (n *Node) applyChanges(ctx context.Context, changes []*api.HeadChange) {
	n.addCacheBlockHeaders(changes)

	idx := -1
	for i := range changes {
		switch changes[i].Type {
		case store.HCCurrent, store.HCApply:
			idx = i
		}
	}

	if idx == -1 {
		return
	}

	ts := changes[idx].Val

	cctx, cancel := context.WithTimeout(ctx, n.opt.APITimeout)
	weight, err := n.upstream.full.ChainTipSetWeight(cctx, ts.Key())
	cancel()

	if err != nil {
		log.Errorf("node %s call ChainTipSetWeight error: %s", n.address, err)
		return
	}

	start := time.Now()
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case n.headCh <- &headCandidate{
			node:   n,
			ts:     ts,
			weight: weight,
		}:
			return
		case now := <-t.C:
			log.Warnf("node %s took too long before we can send the new head change, ts=%s, height=%d, weight=%s, delay=%s",
				n.address, ts.Key(), ts.Height(), weight, now.Sub(start))
		}
	}
}

func (n *Node) loadTipset(ctx context.Context, tsk types.TipSetKey) (*types.TipSet, error) {
	reqCtx, cancel := context.WithTimeout(ctx, n.opt.APITimeout)
	defer cancel()

	cids := tsk.Cids()
	bhs := make([]*types.BlockHeader, 0)
	var wg sync.WaitGroup
	var lock sync.Mutex
	var e error
	wg.Add(len(cids))

	for _, c := range cids {
		go func(ci cid.Cid) {
			defer wg.Done()
			bh, err := n.loadBlockHeader(reqCtx, ci)
			if err != nil {
				e = err
				return
			}
			lock.Lock()
			defer lock.Unlock()
			bhs = append(bhs, bh)
		}(c)
	}
	wg.Wait()

	if e != nil {
		return nil, e
	}

	return types.NewTipSet(bhs)
}

func (n *Node) loadBlockHeader(ctx context.Context, c cid.Cid) (*types.BlockHeader, error) {
	if bh, ok := n.loadCacheBlockHeader(c); ok {
		return bh, nil
	}
	return n.upstream.full.ChainGetBlock(ctx, c)
}
