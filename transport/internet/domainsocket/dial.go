//go:build !windows && !wasm
// +build !windows,!wasm

package domainsocket

import (
	"context"

	"github.com/v2fly/v2ray-core/v5/common"
	"github.com/v2fly/v2ray-core/v5/common/net"
	"github.com/v2fly/v2ray-core/v5/transport/internet"
	"github.com/v2fly/v2ray-core/v5/transport/internet/tls"
)

func Dial(ctx context.Context, dest net.Destination, streamSettings *internet.MemoryStreamConfig) (internet.Connection, error) {
	var addr *net.UnixAddr
	var err error
	if settings, ok := streamSettings.ProtocolSettings.(*Config); ok {
		addr, err = settings.GetUnixAddr()
		if err != nil {
			return nil, err
		}
	} else if dest.Network == net.Network_UNIX {
		addr = &net.UnixAddr{
			Name: dest.Address.Domain(),
			Net:  "unix",
		}
	} else {
		return nil, newError("cannot find domain socket destination from destination ", dest.String()).AtWarning()
	}

	conn, err := net.DialUnix("unix", nil, addr)
	if err != nil {
		return nil, newError("failed to dial unix: ", addr.Name).Base(err).AtWarning()
	}

	if config := tls.ConfigFromStreamSettings(streamSettings); config != nil {
		return tls.Client(conn, config.GetTLSConfig(tls.WithDestination(dest))), nil
	}

	return conn, nil
}

func init() {
	common.Must(internet.RegisterTransportDialer(protocolName, Dial))
}
