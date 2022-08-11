package client

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/pubsub"
	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-cid"
)

const (
	tipsetChangeTopic = "tschange"
)

type Local interface {
	// ChainNotify returns channel with chain head updates.
	// First message is guaranteed to be of len == 1, and type == 'current'.
	ChainNotify(context.Context) (<-chan []*api.HeadChange, error)

	// Signed message and Propagating the signed message to all live nodes
	MpoolPushMessage(context.Context, *types.Message, *api.MessageSendSpec) (*types.SignedMessage, error)
}

var _ Local = (*Coordinator)(nil)

type Coordinator struct {
	headCh         chan *headCandidate
	loseHeartCh    chan string
	receiveHeartCh chan struct {
		addr    string
		version api.Version
	}

	bcache *blockHeaderCache
	headMu sync.RWMutex
	head   *types.TipSet
	weight types.BigInt
	nodes  []string
	sel    *Selector
	tspub  *pubsub.PubSub
}

func NewCoordinator() (*Coordinator, error) {

	bcache, err := newBlockHeaderCache(1 << 20)
	if err != nil {
		return nil, err
	}

	return &Coordinator{
		headCh: make(chan *headCandidate, 256),
		receiveHeartCh: make(chan struct {
			addr    string
			version api.Version
		}, 256),
		loseHeartCh: make(chan string, 256),
		bcache:      bcache,
		nodes:       make([]string, 0),
		sel:         newSelector(),
		tspub:       pubsub.New(256),
	}, nil
}

func (c *Coordinator) RunNode(ctx context.Context, addr string, header http.Header, opt NodeOption) error {

	full, closer, err := client.NewFullNodeRPCV1(ctx, addr, header)
	if err != nil {
		return err
	}

	node := &Node{
		opt:                  opt,
		address:              addr,
		relistenInterval:     opt.ReListenMinInterval,
		headCh:               c.headCh,
		receiveHeartCh:       c.receiveHeartCh,
		loseHeartCh:          c.loseHeartCh,
		loadCacheBlockHeader: c.bcache.load,
		addCacheBlockHeaders: c.bcache.add,
	}
	node.upstream.full = full
	node.upstream.closer = closer

	err = c.sel.RunNode(ctx, node)
	if err != nil {
		return err
	}
	return nil
}

// Start starts the coordinate loop
func (c *Coordinator) Start(ctx context.Context) context.CancelFunc {

	cctx, abort := context.WithCancel(ctx)

	go func() {

		log.Info("start head coordinator loop")
		defer log.Info("stop head coordinator loop")

		for {
			select {
			case <-cctx.Done():
				return
			case hc := <-c.headCh:
				c.handleCandidate(cctx, hc)
			case liveNode := <-c.receiveHeartCh:
				c.sel.markLiveNode(liveNode)
			case addr := <-c.loseHeartCh:
				c.sel.markLoseHeartNode(addr)
			}
		}
	}()

	return abort
}

func (c *Coordinator) RunNodesHeightChecker(ctx context.Context, genesis *types.TipSet) {

	log.Info("run NodeHeightChecker")
	defer log.Info("exit NodeHeightChecker")

	genesisTime := time.Unix(int64(genesis.MinTimestamp()), 0)
	ticker := build.Clock.Ticker(time.Duration(build.BlockDelaySecs) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:

			sinceGenesis := build.Clock.Now().Sub(genesisTime)
			expectedHeight := int64(sinceGenesis.Seconds()) / int64(build.BlockDelaySecs)

			node, err := c.sel.Select()
			if err != nil {
				log.Errorf("Select Node err: %v", err)
				continue
			}

			time.Sleep(time.Duration(build.PropagationDelaySecs))
			cctx, abort := context.WithTimeout(ctx, time.Duration(build.BlockDelaySecs-build.PropagationDelaySecs))
			ts, err := node.upstream.full.ChainHead(cctx)
			abort()

			if err != nil {
				log.Errorf("Check Node %v Height, Call ChainHead err:%v", node.address, err)
				continue
			}

			if ts == nil {
				continue
			}

			head := int64(ts.Height())
			if head < expectedHeight {
				log.Warnf("Selected Node %v don't match Height, expectedHeight %d headHeight:%d", node.address,
					expectedHeight, head)
			}
		}

	}
}

func (c *Coordinator) Select() (v1api.FullNode, error) {
	n, err := c.sel.Select()
	if err != nil {
		return nil, err
	}
	return n.upstream.full, nil
}

func (c *Coordinator) handleCandidate(ctx context.Context, hc *headCandidate) {
	addr := hc.node.address

	c.headMu.Lock()
	defer c.headMu.Unlock()

	//1. more weight
	//2. if equal weight. select more blocks
	if c.head == nil || hc.weight.GreaterThan(c.weight) || (hc.weight.Equals(c.weight) && len(hc.ts.Blocks()) > len(c.head.Blocks())) {

		if c.head != nil {
			prev := c.head
			next := hc.ts
			headChanges, err := c.applyTipSetChange(ctx, prev, next, hc.node) //todo if network become slow
			if err != nil {
				log.Errorf("apply tipset change: %s", err)
			}
			if headChanges == nil {
				return
			}
			c.tspub.Pub(headChanges, tipsetChangeTopic)
		}

		c.head = hc.ts
		c.weight = hc.weight
		c.nodes = append(c.nodes[:0], addr)
		c.sel.markBestNodes(addr)

		return
	}

	if c.head.Equals(hc.ts) {
		contains := false
		for ni := range c.nodes {
			if c.nodes[ni] == addr {
				contains = true
				break
			}
		}

		if !contains {
			c.nodes = append(c.nodes, addr)
			c.sel.markBestNodes(c.nodes...)

		}
		return
	}
}

func (c *Coordinator) applyTipSetChange(ctx context.Context, prev, next *types.TipSet, node *Node) ([]*api.HeadChange, error) {
	revert, apply, err := store.ReorgOps(ctx, node.loadTipset, prev, next)
	if err != nil {
		return nil, err
	}

	hc := make([]*api.HeadChange, 0, len(revert)+len(apply))
	for i := range revert {
		hc = append(hc, &api.HeadChange{
			Type: store.HCRevert,
			Val:  revert[i],
		})
	}

	for i := range apply {
		hc = append(hc, &api.HeadChange{
			Type: store.HCApply,
			Val:  apply[i],
		})
	}

	if len(hc) == 0 {
		return nil, nil
	}
	return hc, nil
}

// ChainNotify impls api.FullNode.ChainNotify
func (c *Coordinator) ChainNotify(ctx context.Context) (<-chan []*api.HeadChange, error) {
	subch := c.tspub.Sub(tipsetChangeTopic)

	c.headMu.RLock()
	head := c.head
	c.headMu.RUnlock()

	out := make(chan []*api.HeadChange, 32)
	out <- []*api.HeadChange{{
		Type: store.HCCurrent,
		Val:  head,
	}}

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
		}

		close(done)
	}()

	go func() {
		defer func() {
			close(out)
			c.tspub.Unsub(subch)
			for range subch {
			}
		}()

		for {
			select {
			case val, ok := <-subch:
				if !ok {
					log.Info("ChainNotify: request done")
					return
				}

				if len(out) > 0 {
					log.Warnf("ChainNotify: head change sub is slow, has %d buffered entries", len(out))
				}

				select {
				case out <- val.([]*api.HeadChange):

				case <-done:
					return

				case <-time.After(time.Minute):
					log.Warn("ChainNotify: stucked for 1min")
					return
				}

			case <-done:
				return
			}
		}
	}()

	return out, nil
}

func (c *Coordinator) MpoolPushMessage(context context.Context, msg *types.Message, spec *api.MessageSendSpec) (*types.SignedMessage, error) {

	selectedNode, err := c.sel.Select()
	if err != nil {
		return nil, err
	}

	var sm *types.SignedMessage
	sm, err = selectedNode.upstream.full.MpoolPushMessage(context, msg, spec)
	if err != nil {
		return nil, err
	}

	nodes, err := c.sel.GetAllLiveNodes()
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	wg.Add(len(nodes))
	for _, node := range nodes {
		go func(n *Node) {
			defer wg.Done()

			if n.address == selectedNode.address {
				return
			}

			_, err := n.upstream.full.MpoolPush(context, sm)
			if err != nil {
				log.Warnf("propagating signed message to node %v error: %v", n.address, err)
			}
		}(node)
	}

	wg.Wait()

	return sm, nil
}

type headCandidate struct {
	node   *Node
	ts     *types.TipSet
	weight types.BigInt
}

func newBlockHeaderCache(size int) (*blockHeaderCache, error) {
	cache, err := lru.New2Q(size)
	if err != nil {
		return nil, err
	}

	return &blockHeaderCache{
		cache: cache,
	}, nil
}

type blockHeaderCache struct {
	cache *lru.TwoQueueCache
}

func (bc *blockHeaderCache) add(changes []*api.HeadChange) {
	for _, hc := range changes {
		blks := hc.Val.Blocks()
		for bi := range blks {
			bc.cache.Add(blks[bi].Cid(), blks[bi])
		}
	}
}

func (bc *blockHeaderCache) load(c cid.Cid) (*types.BlockHeader, bool) {
	val, ok := bc.cache.Get(c)
	if !ok {
		return nil, false
	}

	blk, ok := val.(*types.BlockHeader)
	return blk, ok
}
