package client

import (
	"context"
	"fmt"
	"math/rand"
	"sync"

	"github.com/filecoin-project/lotus/api"
	"golang.org/x/xerrors"
)

// common errors
var (
	ErrNoNodeAvailable = fmt.Errorf("no node available")
)

type MajorMinorVersion struct {
	major uint32
	minor uint32
}

func fromVersion(v api.Version) MajorMinorVersion {
	major, minor, _ := v.Ints()
	return MajorMinorVersion{
		major: major,
		minor: minor,
	}
}

type Selector struct {
	prior struct {
		sync.RWMutex
		addrs []string
	}

	live struct {
		sync.RWMutex
		addrs              []string
		addrVersions       map[string]api.Version
		versions           map[api.Version][]string
		majorMinorVersions map[MajorMinorVersion][]string
	}

	all struct {
		sync.RWMutex
		addrs []string
		nodes map[string]struct {
			*Node
			abort context.CancelFunc
		}
	}
}

func newSelector() *Selector {
	sel := new(Selector)

	sel.prior.addrs = make([]string, 0)

	sel.live.addrs = make([]string, 0)
	sel.live.addrVersions = make(map[string]api.Version)
	sel.live.versions = make(map[api.Version][]string)
	sel.live.majorMinorVersions = make(map[MajorMinorVersion][]string)

	sel.all.addrs = make([]string, 0)
	sel.all.nodes = make(map[string]struct {
		*Node
		abort context.CancelFunc
	})
	return sel
}

func (s *Selector) RunNode(ctx context.Context, node *Node) error {

	if _, exist := s.all.nodes[node.address]; exist {
		return nil
	}

	apiVersion, err := node.CheckHealth(ctx)
	if err != nil {
		return err
	}

	s.markLiveNode(struct {
		addr    string
		version api.Version
	}{
		addr:    node.address,
		version: apiVersion,
	})

	s.all.Lock()
	s.all.addrs = append(s.all.addrs, node.address)
	s.all.nodes[node.address] = struct {
		*Node
		abort context.CancelFunc
	}{
		Node:  node,
		abort: node.Start(ctx),
	}
	s.all.Unlock()

	return nil
}

func (s *Selector) StopNodes(nodeAddrs []string) {

	for _, nodeAddr := range nodeAddrs {
		if _, exist := s.all.nodes[nodeAddr]; !exist {
			continue
		}

		s.all.Lock()
		s.all.addrs = remove(s.all.addrs, nodeAddr)
		s.all.nodes[nodeAddr].abort()
		delete(s.all.nodes, nodeAddr)
		s.all.Unlock()

		s.prior.Lock()
		s.prior.addrs = remove(s.prior.addrs, nodeAddr)
		s.prior.Unlock()

		s.live.Lock()
		version := s.live.addrVersions[nodeAddr]
		s.live.addrs = remove(s.live.addrs, nodeAddr)
		delete(s.live.addrVersions, nodeAddr)
		s.live.versions[version] = remove(s.live.versions[version], nodeAddr)
		s.live.majorMinorVersions[fromVersion(version)] = remove(s.live.majorMinorVersions[fromVersion(version)], nodeAddr)
		s.live.Unlock()
	}
}

func remove(s []string, r string) []string {
	for i, v := range s {
		if v == r {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}

func contain(s []string, r string) bool {
	for _, v := range s {
		if v == r {
			return true
		}
	}
	return false
}

func (s *Selector) StopAllNode() {

	s.all.Lock()
	s.all.addrs = s.all.addrs[:0]
	for _, addr := range s.all.addrs {
		s.all.nodes[addr].abort()
		delete(s.all.nodes, addr)
	}
	s.all.Unlock()

	s.prior.Lock()
	s.prior.addrs = s.prior.addrs[:0]
	s.prior.Unlock()

	s.live.Lock()
	s.live.addrs = s.live.addrs[:0]
	s.live.addrVersions = make(map[string]api.Version)
	s.live.versions = make(map[api.Version][]string)
	s.live.majorMinorVersions = make(map[MajorMinorVersion][]string)
	s.live.Unlock()
}

func (s *Selector) markBestNodes(addrs ...string) {

	s.prior.Lock()
	s.prior.addrs = append(s.prior.addrs[:0], addrs...)
	s.prior.Unlock()

}

func (s *Selector) markLiveNode(liveNode struct {
	addr    string
	version api.Version
}) {

	addr := liveNode.addr
	version := liveNode.version
	if _, exist := s.live.addrVersions[addr]; exist {
		return
	}

	s.live.Lock()
	defer s.live.Unlock()

	if _, exist := s.live.addrVersions[addr]; exist {
		return
	}

	log.Infof("mark live Node %v: %v", liveNode.addr, liveNode.version)
	s.live.addrs = append(s.live.addrs, addr)
	s.live.addrVersions[addr] = version

	oldVersionAddrs := s.live.versions[version]
	if oldVersionAddrs == nil {
		oldVersionAddrs = []string{addr}
	} else {
		oldVersionAddrs = append(oldVersionAddrs, addr)
	}
	s.live.versions[version] = oldVersionAddrs

	oldMajorMinorAddrs := s.live.majorMinorVersions[fromVersion(version)]
	if oldMajorMinorAddrs == nil {
		oldMajorMinorAddrs = []string{addr}
	} else {
		oldMajorMinorAddrs = append(oldMajorMinorAddrs, addr)
	}
	s.live.majorMinorVersions[fromVersion(version)] = oldMajorMinorAddrs
}

func (s *Selector) markLoseHeartNode(addr string) {

	s.live.RLock()
	if _, exist := s.live.addrVersions[addr]; !exist {
		s.live.RUnlock()
		return
	}
	s.live.RUnlock()

	log.Warnf("mark loseHeart Node %s", addr)

	s.prior.Lock()
	s.prior.addrs = remove(s.prior.addrs, addr)
	s.prior.Unlock()

	s.live.Lock()
	version := s.live.addrVersions[addr]
	s.live.addrs = remove(s.live.addrs, addr)
	delete(s.live.addrVersions, addr)
	s.live.versions[version] = remove(s.live.versions[version], addr)
	s.live.majorMinorVersions[fromVersion(version)] = remove(s.live.majorMinorVersions[fromVersion(version)], addr)
	s.live.Unlock()
}

func (s *Selector) GetAllLiveNodes() ([]*Node, error) {

	s.all.RLock()
	defer s.all.RUnlock()

	s.live.RLock()
	defer s.live.RUnlock()

	allLiveAddrs := s.live.addrs
	if len(allLiveAddrs) == 0 {
		return nil, xerrors.New("No live nodes found")
	}

	allLiveNodes := []*Node{}
	for _, liveAddr := range allLiveAddrs {
		if n, exist := s.all.nodes[liveAddr]; !exist {
			continue
		} else {
			allLiveNodes = append(allLiveNodes, n.Node)
		}
	}
	return allLiveNodes, nil
}

// Select tries to choose a node from the candidates
func (s *Selector) Select() (*Node, error) {

	var priorAddrs []string
	clientAPIVersion := api.FullAPIVersion1
	clientMajorMinorVersion := fromVersion(clientAPIVersion)

	s.prior.RLock()
	priorAddrs = s.prior.addrs
	s.prior.RUnlock()

	s.all.RLock()
	defer s.all.RUnlock()

	s.live.RLock()
	defer s.live.RUnlock()

	//1.Select nodes with the same version from the best chain address
	if len(priorAddrs) != 0 {
		targetNodes := make([]*Node, 0)
		for _, priorAddr := range priorAddrs {
			if version, exist := s.live.addrVersions[priorAddr]; exist {
				if version == clientAPIVersion {
					if node, ok := s.all.nodes[priorAddr]; ok {
						log.Debugf("Choose the best chain node with the same API version:%v, Client:%v Server:%v", priorAddr,
							clientAPIVersion, version)
						targetNodes = append(targetNodes, node.Node)
					}
				}
			}
		}
		if len(targetNodes) != 0 {
			return targetNodes[rand.Intn(len(targetNodes))], nil
		}
	}

	//2.Randomly select a node from nodes with the same version
	sameVerLiveAddrs := s.live.versions[clientAPIVersion]
	sameVerLiveSize := len(sameVerLiveAddrs)
	if sameVerLiveSize != 0 {
		randSameVerLiveAddr := sameVerLiveAddrs[rand.Intn(sameVerLiveSize)]
		if node, ok := s.all.nodes[randSameVerLiveAddr]; ok {
			log.Debugf("Select live node with the same API version:%v", randSameVerLiveAddr)
			return node.Node, nil
		}
	}

	//3.Randomly select a node from the compatible version nodes
	compatibleVerLiveAddrs := s.live.majorMinorVersions[clientMajorMinorVersion]
	compatibleVerLiveSize := len(compatibleVerLiveAddrs)
	if compatibleVerLiveSize != 0 {
		randCompatibleVerLiveAddr := compatibleVerLiveAddrs[rand.Intn(compatibleVerLiveSize)]
		if node, ok := s.all.nodes[randCompatibleVerLiveAddr]; ok {
			log.Warnf("No node with the same API version found! Select a compatible live node:%v, Client:%v Server:%v", randCompatibleVerLiveAddr,
				clientAPIVersion, s.live.addrVersions[randCompatibleVerLiveAddr])
			return node.Node, nil
		}
	}

	//4.If no node with the same version/compatible version is found, a node is randomly selected from all live nodes
	allLiveSize := len(s.live.addrs)
	if allLiveSize != 0 {
		randLiveAddr := s.live.addrs[rand.Intn(allLiveSize)]
		if node, ok := s.all.nodes[randLiveAddr]; ok {
			log.Warnf("No node with the same version/compatible version found!! Select a live node:%v, Client:%v Server:%v", randLiveAddr, clientAPIVersion,
				s.live.addrVersions[randLiveAddr])
			return node.Node, nil
		}
	}

	//5.If no node with the same version/compatible version/live state is found, a node is randomly selected from all nodes
	allSize := len(s.all.addrs)
	if allSize == 0 {
		return nil, ErrNoNodeAvailable
	}
	randAddr := s.all.addrs[rand.Intn(allSize)]
	log.Errorf("No node with the same version/compatible version/live state found!!!  Select node:%v", randAddr)
	return s.all.nodes[randAddr].Node, nil
}
