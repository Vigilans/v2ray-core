package udp

import (
	"context"

	"github.com/v2fly/v2ray-core/v5/common/buf"
	"github.com/v2fly/v2ray-core/v5/common/net"
	"github.com/v2fly/v2ray-core/v5/common/protocol/udp"
	"github.com/v2fly/v2ray-core/v5/common/session"
	"github.com/v2fly/v2ray-core/v5/transport/internet"
)

type HubOption func(h *Hub)

func HubCapacity(capacity int) HubOption {
	return func(h *Hub) {
		h.capacity = capacity
	}
}

func HubReceiveOriginalDestination(r bool) HubOption {
	return func(h *Hub) {
		h.recvOrigDest = r
	}
}

type Hub struct {
	conn         net.PacketConn
	cache        chan *udp.Packet
	capacity     int
	recvOrigDest bool
}

func ListenUDP(ctx context.Context, address net.Address, port net.Port, streamSettings *internet.MemoryStreamConfig, options ...HubOption) (*Hub, error) {
	hub := &Hub{
		capacity:     256,
		recvOrigDest: false,
	}
	for _, opt := range options {
		opt(hub)
	}

	var sockopt *internet.SocketConfig
	if streamSettings != nil {
		sockopt = streamSettings.SocketSettings
	}
	if sockopt != nil && sockopt.ReceiveOriginalDestAddress {
		hub.recvOrigDest = true
	}

	if address.Family().IsDomain() && (address.Domain()[0] == '/' || address.Domain()[0] == '@') && port == net.Port(0) { // unix
		dsConn, err := internet.ListenSystemPacket(ctx, &net.UnixAddr{
			Name: address.Domain(),
			Net:  "unixgram",
		}, sockopt)
		if err != nil {
			return nil, newError("failed to listen Unixgram Domain Socket on ", address).Base(err)
		}
		if _, ok := dsConn.(unixConn); !ok {
			return nil, newError("returned PacketConn is not *net.UnixConn", address).Base(err)
		}
		newError("listening Unixgram Domain Socket on ", address).WriteToLog(session.ExportIDToError(ctx))
		hub.conn = dsConn
	} else { // udp
		udpConn, err := internet.ListenSystemPacket(ctx, &net.UDPAddr{
			IP:   address.IP(),
			Port: int(port),
		}, sockopt)
		if err != nil {
			return nil, newError("failed to listen UDP on ", address, ":", port).Base(err)
		}
		if _, ok := udpConn.(*net.UDPConn); !ok {
			return nil, newError("returned PacketConn is not *net.UDPConn", address).Base(err)
		}
		newError("listening UDP on ", address, ":", port).WriteToLog(session.ExportIDToError(ctx))
		hub.conn = udpConn
	}

	hub.cache = make(chan *udp.Packet, hub.capacity)
	go hub.start()
	return hub, nil
}

// Close implements net.Listener.
func (h *Hub) Close() error {
	h.conn.Close()
	return nil
}

func (h *Hub) WriteTo(payload []byte, dest net.Destination) (int, error) {
	switch conn := h.conn.(type) {
	case *net.UDPConn:
		if dest.Network == net.Network_UDP {
			return conn.WriteToUDP(payload, &net.UDPAddr{IP: dest.Address.IP(), Port: int(dest.Port)})
		}
	case unixConn:
		if dest.Network == net.Network_UNIXGRAM {
			return conn.WriteToUnix(payload, &net.UnixAddr{Name: dest.Address.Domain(), Net: "unixgram"})
		}
	}
	return 0, newError("failed to write to destination due to network mismatch: ", dest)
}

func (h *Hub) start() {
	c := h.cache
	defer close(c)

	oobBytes := make([]byte, 256)

	for {
		var n, noob int
		var addr net.Addr
		var err error

		buffer := buf.New()
		rawBytes := buffer.Extend(buf.Size)

		switch conn := h.conn.(type) {
		case *net.UDPConn:
			n, noob, _, addr, err = ReadUDPMsg(conn, rawBytes, oobBytes)
		case unixConn:
			n, noob, _, addr, err = conn.ReadMsgUnix(rawBytes, oobBytes)
		}

		if err != nil {
			newError("failed to read UDP msg").Base(err).WriteToLog()
			buffer.Release()
			break
		}
		buffer.Resize(0, int32(n))

		if buffer.IsEmpty() {
			buffer.Release()
			continue
		}

		source := net.DestinationFromAddr(addr)
		payload := &udp.Packet{
			Payload: buffer,
			Source:  source,
		}
		if source.Network == net.Network_UDP && h.recvOrigDest && noob > 0 {
			payload.Target = RetrieveOriginalDest(oobBytes[:noob])
			if payload.Target.IsValid() {
				newError("UDP original destination: ", payload.Target).AtDebug().WriteToLog()
			} else {
				newError("failed to read UDP original destination").WriteToLog()
			}
		}

		select {
		case c <- payload:
		default:
			buffer.Release()
			payload.Payload = nil
		}
	}
}

// Addr implements net.Listener.
func (h *Hub) Addr() net.Addr {
	return h.conn.LocalAddr()
}

func (h *Hub) Receive() <-chan *udp.Packet {
	return h.cache
}

type unixConn interface {
	ReadMsgUnix(b, oob []byte) (n, oobn, flags int, addr *net.UnixAddr, err error)
	WriteToUnix(b []byte, addr *net.UnixAddr) (int, error)
}
