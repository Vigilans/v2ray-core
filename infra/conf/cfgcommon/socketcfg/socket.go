package socketcfg

import (
	"strings"

	"github.com/v2fly/v2ray-core/v5/transport/internet"
)

type SocketConfig struct {
	Mark                 uint32 `json:"mark"`
	TFO                  *bool  `json:"tcpFastOpen"`
	TProxy               string `json:"tproxy"`
	AcceptProxyProtocol  bool   `json:"acceptProxyProtocol"`
	TCPKeepAliveInterval int32  `json:"tcpKeepAliveInterval"`
	TCPKeepAliveIdle     int32  `json:"tcpKeepAliveIdle"`
	TFOQueueLength       uint32 `json:"tcpFastOpenQueueLength"`
	BindToDevice         string `json:"bindToDevice"`
	RxBufSize            uint64 `json:"rxBufSize"`
	TxBufSize            uint64 `json:"txBufSize"`
	ForceBufSize         bool   `json:"forceBufSize"`
	ListenStrategy       string `json:"listenStrategy"`
}

// Build implements Buildable.
func (c *SocketConfig) Build() (*internet.SocketConfig, error) {
	var tfoSettings internet.SocketConfig_TCPFastOpenState
	if c.TFO != nil {
		if *c.TFO {
			tfoSettings = internet.SocketConfig_Enable
		} else {
			tfoSettings = internet.SocketConfig_Disable
		}
	}

	tfoQueueLength := c.TFOQueueLength
	if tfoQueueLength == 0 {
		tfoQueueLength = 4096
	}

	var tproxy internet.SocketConfig_TProxyMode
	switch strings.ToLower(c.TProxy) {
	case "tproxy":
		tproxy = internet.SocketConfig_TProxy
	case "redirect":
		tproxy = internet.SocketConfig_Redirect
	default:
		tproxy = internet.SocketConfig_Off
	}

	var listenStrategy internet.SocketConfig_ListenStrategy
	switch strings.ToLower(c.ListenStrategy) {
	case "useip4", "useipv4", "use_ip4", "use_ipv4", "use_ip_v4", "use-ip4", "use-ipv4", "use-ip-v4":
		listenStrategy = internet.SocketConfig_USE_IP4
	case "useip6", "useipv6", "use_ip6", "use_ipv6", "use_ip_v6", "use-ip6", "use-ipv6", "use-ip-v6":
		listenStrategy = internet.SocketConfig_USE_IP6
	case "useip", "use_ip", "use-ip":
		listenStrategy = internet.SocketConfig_USE_IP
	default:
		listenStrategy = internet.SocketConfig_USE_IP
	}

	return &internet.SocketConfig{
		Mark:                 c.Mark,
		Tfo:                  tfoSettings,
		TfoQueueLength:       tfoQueueLength,
		Tproxy:               tproxy,
		AcceptProxyProtocol:  c.AcceptProxyProtocol,
		TcpKeepAliveInterval: c.TCPKeepAliveInterval,
		TcpKeepAliveIdle:     c.TCPKeepAliveIdle,
		RxBufSize:            int64(c.RxBufSize),
		TxBufSize:            int64(c.TxBufSize),
		ForceBufSize:         c.ForceBufSize,
		BindToDevice:         c.BindToDevice,
		ListenStrategy:       listenStrategy,
	}, nil
}
