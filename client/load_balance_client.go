package client

import (
	"context"
	"fmt"
	"net/http"
	"reflect"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/types"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("load_balance_client")
var (
	errorType   = reflect.TypeOf(new(error)).Elem()
	contextType = reflect.TypeOf(new(context.Context)).Elem()
)

func NewLoadBalanceFullNodeRPCV1(ctx context.Context, addrs []string, requestHeaders []http.Header) (v1api.FullNode, jsonrpc.ClientCloser, error) {

	var res v1api.FullNodeStruct

	closer, err := NewLoadBalanceMergeClient(ctx, addrs, requestHeaders, api.GetInternalStructs(&res))
	if err != nil {
		return nil, nil, err
	}
	return &res, closer, nil
}

func NewLoadBalanceMergeClient(ctx context.Context, addrs []string, requestHeaders []http.Header, outs []interface{}) (jsonrpc.ClientCloser, error) {

	coordinator, err := NewCoordinator()
	if err != nil {
		return nil, err
	}
	cf := coordinator.Start(ctx)

	defaultNodeConf := DefaultNodeOption()
	availableNodeCount := 0
	for idx, addr := range addrs {
		if addr == "" {
			continue
		}
		err = coordinator.RunNode(ctx, addrs[idx], requestHeaders[idx], defaultNodeConf)
		if err != nil {
			log.Warnf("coordinator.RunNode %s fail: %s", addrs[idx], err)
			continue
		}
		availableNodeCount++
	}

	if availableNodeCount == 0 {
		cf()
		return nil, xerrors.Errorf("No available Node")
	}

	var fullAPI v1api.FullNode
	fullAPI, err = coordinator.Select()
	if err != nil {
		return nil, err
	}

	var genesisTs *types.TipSet
	genesisTs, err = fullAPI.ChainGetGenesis(ctx)
	if err != nil {
		return nil, err
	}

	go coordinator.RunNodesHeightChecker(ctx, genesisTs)

	var localAPI Local = coordinator
	localAPIMethodTypes := make(map[string]reflect.Type)
	localAPIType := reflect.TypeOf((*Local)(nil)).Elem()
	for i := 0; i < localAPIType.NumMethod(); i++ {
		method := localAPIType.Method(i)
		localAPIMethodTypes[method.Name] = method.Type
	}

	l := loadBalanceClient{
		localAPI:            localAPI,
		localAPIMechodTypes: localAPIMethodTypes,
		coordinator:         coordinator,
	}

	if err := l.provide(outs); err != nil {
		cf()
		return nil, err
	}
	return jsonrpc.ClientCloser(cf), nil
}

type loadBalanceClient struct {
	localAPI            Local
	localAPIMechodTypes map[string]reflect.Type
	coordinator         *Coordinator
}

func (l *loadBalanceClient) provide(outs []interface{}) error {
	for _, handler := range outs {
		//Type Check
		t := reflect.TypeOf(handler)
		if t.Kind() != reflect.Pointer {
			return xerrors.Errorf("expect handler to be a pointer")
		}
		te := t.Elem()
		if te.Kind() != reflect.Struct {
			return xerrors.Errorf("handler should be a struct")
		}

		handlerValue := reflect.ValueOf(handler)
		for i := 0; i < te.NumField(); i++ {
			tef := te.Field(i)
			fn, err := l.makeRpcFunc(tef)
			if err != nil {
				return err
			}
			handlerValue.Elem().Field(i).Set(fn)
		}

	}
	return nil
}

func (l *loadBalanceClient) makeRpcFunc(field reflect.StructField) (reflect.Value, error) {
	ftyp := field.Type
	if ftyp.Kind() != reflect.Func {
		return reflect.Value{}, xerrors.Errorf("handler field must be func")
	}

	fn := &rpcFunc{
		ftyp:  ftyp,
		name:  field.Name,
		retry: field.Tag.Get("retry") == "true",
	}
	fn.valOut, fn.errOut, fn.nout = processFuncOut(ftyp)

	if ftyp.NumIn() > 0 && ftyp.In(0) == contextType {
		fn.hasCtx = 1
	}
	fn.returnValueIsChannel = fn.valOut != -1 && ftyp.Out(fn.valOut).Kind() == reflect.Chan

	return reflect.MakeFunc(ftyp, func(args []reflect.Value) (results []reflect.Value) {

		client, err := l.coordinator.Select()
		if err != nil {
			out := make([]reflect.Value, fn.nout)
			if fn.valOut != -1 {
				out[fn.valOut] = reflect.New(ftyp.Out(fn.valOut)).Elem()
			}
			if fn.errOut != -1 {
				out[fn.errOut] = reflect.ValueOf(&err).Elem()
			}
			return out
		}

		if methodType, exist := l.localAPIMechodTypes[field.Name]; exist {
			if methodType == ftyp {
				return reflect.ValueOf(l.localAPI).MethodByName(field.Name).Call(args)
			}
		}

		clientVal := reflect.ValueOf(client)
		clientMethod := clientVal.MethodByName(field.Name)
		return clientMethod.Call(args)

	}), nil
}

type rpcFunc struct {
	ftyp reflect.Type
	name string

	nout   int
	valOut int
	errOut int

	hasCtx               int
	returnValueIsChannel bool

	retry bool
}

func processFuncOut(funcType reflect.Type) (valOut int, errOut int, n int) {
	errOut = -1 // -1 if not found
	valOut = -1
	n = funcType.NumOut()

	switch n {
	case 0:
	case 1:
		if funcType.Out(0) == errorType {
			errOut = 0
		} else {
			valOut = 0
		}
	case 2:
		valOut = 0
		errOut = 1
		if funcType.Out(1) != errorType {
			panic("expected error as second return value")
		}
	default:
		errstr := fmt.Sprintf("too many return values: %s", funcType)
		panic(errstr)
	}

	return
}
