package inbound

import (
	"context"

	core "github.com/v2fly/v2ray-core/v5"
	"github.com/v2fly/v2ray-core/v5/app/proxyman"
	"github.com/v2fly/v2ray-core/v5/common"
	"github.com/v2fly/v2ray-core/v5/common/dice"
	"github.com/v2fly/v2ray-core/v5/common/errors"
	"github.com/v2fly/v2ray-core/v5/common/mux"
	"github.com/v2fly/v2ray-core/v5/common/net"
	"github.com/v2fly/v2ray-core/v5/features/policy"
	"github.com/v2fly/v2ray-core/v5/features/stats"
	"github.com/v2fly/v2ray-core/v5/proxy"
	"github.com/v2fly/v2ray-core/v5/transport/internet"
)

func getStatCounter(v *core.Instance, tag string) (stats.Counter, stats.Counter) {
	var uplinkCounter stats.Counter
	var downlinkCounter stats.Counter

	policy := v.GetFeature(policy.ManagerType()).(policy.Manager)
	if len(tag) > 0 && policy.ForSystem().Stats.InboundUplink {
		statsManager := v.GetFeature(stats.ManagerType()).(stats.Manager)
		name := "inbound>>>" + tag + ">>>traffic>>>uplink"
		c, _ := stats.GetOrRegisterCounter(statsManager, name)
		if c != nil {
			uplinkCounter = c
		}
	}
	if len(tag) > 0 && policy.ForSystem().Stats.InboundDownlink {
		statsManager := v.GetFeature(stats.ManagerType()).(stats.Manager)
		name := "inbound>>>" + tag + ">>>traffic>>>downlink"
		c, _ := stats.GetOrRegisterCounter(statsManager, name)
		if c != nil {
			downlinkCounter = c
		}
	}

	return uplinkCounter, downlinkCounter
}

type AlwaysOnInboundHandler struct {
	proxy   proxy.Inbound
	workers []worker
	mux     *mux.Server
	tag     string
}

func NewAlwaysOnInboundHandler(ctx context.Context, tag string, receiverConfig *proxyman.ReceiverConfig, proxyConfig interface{}) (*AlwaysOnInboundHandler, error) {
	rawProxy, err := common.CreateObject(ctx, proxyConfig)
	if err != nil {
		return nil, err
	}
	p, ok := rawProxy.(proxy.Inbound)
	if !ok {
		return nil, newError("not an inbound proxy.")
	}

	h := &AlwaysOnInboundHandler{
		proxy: p,
		mux:   mux.NewServer(ctx),
		tag:   tag,
	}

	uplinkCounter, downlinkCounter := getStatCounter(core.MustFromContext(ctx), tag)

	network := p.Network()
	portRange := receiverConfig.PortRange
	address := receiverConfig.Listen.AsAddress()
	if address == nil {
		address = net.AnyIP
	}

	mss, err := internet.ToMemoryStreamConfig(receiverConfig.StreamSettings)
	if err != nil {
		return nil, newError("failed to parse stream config").Base(err).AtWarning()
	}

	newWorker := func(listenOn net.Destination) worker {
		switch listenOn.Network {
		case net.Network_TCP:
			newError("creating tcp worker on ", listenOn.NetAddr()).AtDebug().WriteToLog()

			return &tcpWorker{
				address:         listenOn.Address,
				port:            listenOn.Port,
				proxy:           p,
				stream:          mss,
				recvOrigDest:    receiverConfig.ReceiveOriginalDestination,
				tag:             tag,
				dispatcher:      h.mux,
				sniffingConfig:  receiverConfig.GetEffectiveSniffingSettings(),
				uplinkCounter:   uplinkCounter,
				downlinkCounter: downlinkCounter,
				ctx:             ctx,
			}
		case net.Network_UDP:
			newError("creating udp worker on ", listenOn.NetAddr()).AtDebug().WriteToLog()

			return &udpWorker{
				ctx:             ctx,
				tag:             tag,
				proxy:           p,
				address:         listenOn.Address,
				port:            listenOn.Port,
				dispatcher:      h.mux,
				sniffingConfig:  receiverConfig.GetEffectiveSniffingSettings(),
				uplinkCounter:   uplinkCounter,
				downlinkCounter: downlinkCounter,
				stream:          mss,
			}
		case net.Network_UNIX:
			newError("creating unix domain socket worker on ", listenOn.NetAddr()).AtDebug().WriteToLog()

			return &tcpWorker{
				address:         listenOn.Address,
				port:            net.Port(0),
				proxy:           p,
				stream:          mss,
				tag:             tag,
				dispatcher:      h.mux,
				sniffingConfig:  receiverConfig.GetEffectiveSniffingSettings(),
				uplinkCounter:   uplinkCounter,
				downlinkCounter: downlinkCounter,
				ctx:             ctx,
			}
		case net.Network_UNIXGRAM:
			newError("creating unixgram domain socket worker on ", listenOn.NetAddr()).AtDebug().WriteToLog()

			return &udpWorker{
				ctx:             ctx,
				tag:             tag,
				proxy:           p,
				address:         listenOn.Address,
				port:            net.Port(0),
				dispatcher:      h.mux,
				sniffingConfig:  receiverConfig.GetEffectiveSniffingSettings(),
				uplinkCounter:   uplinkCounter,
				downlinkCounter: downlinkCounter,
				stream:          mss,
			}
		}
		return nil
	}

	receivers := make([]net.Destination, 0)
	if portRange != nil {
		for port := portRange.From; port <= portRange.To; port++ {
			if net.HasNetwork(network, net.Network_TCP) {
				receivers = append(receivers, net.TCPDestination(address, net.Port(port)))
			}
			if net.HasNetwork(network, net.Network_UDP) {
				receivers = append(receivers, net.UDPDestination(address, net.Port(port)))
			}
		}
	} else {
		if net.HasNetwork(network, net.Network_UNIX) {
			receivers = append(receivers, net.UnixDestination(address))
		} else if net.HasNetwork(network, net.Network_UNIXGRAM) { // `unixgram` will be used on domain socket only if `unix` is not used, since `unix` and `unixgram` cannot be applied on same file
			receivers = append(receivers, net.UnixgramDestination(address))
		}
	}

	for _, receiver := range receivers {
		h.workers = append(h.workers, newWorker(receiver))
	}

	return h, nil
}

// Start implements common.Runnable.
func (h *AlwaysOnInboundHandler) Start() error {
	for _, worker := range h.workers {
		if err := worker.Start(); err != nil {
			return err
		}
	}
	return nil
}

// Close implements common.Closable.
func (h *AlwaysOnInboundHandler) Close() error {
	var errs []error
	for _, worker := range h.workers {
		errs = append(errs, worker.Close())
	}
	errs = append(errs, h.mux.Close())
	if err := errors.Combine(errs...); err != nil {
		return newError("failed to close all resources").Base(err)
	}
	return nil
}

func (h *AlwaysOnInboundHandler) GetRandomInboundProxy() (interface{}, net.Port, int) {
	if len(h.workers) == 0 {
		return nil, 0, 0
	}
	w := h.workers[dice.Roll(len(h.workers))]
	return w.Proxy(), w.Port(), 9999
}

func (h *AlwaysOnInboundHandler) Tag() string {
	return h.tag
}

func (h *AlwaysOnInboundHandler) GetInbound() proxy.Inbound {
	return h.proxy
}
