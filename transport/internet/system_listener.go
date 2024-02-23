package internet

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pires/go-proxyproto"

	"github.com/v2fly/v2ray-core/v5/common/net"
	"github.com/v2fly/v2ray-core/v5/common/session"
)

var effectiveListener = DefaultListener{}

type controller func(network, address string, fd uintptr) error

type DefaultListener struct {
	controllers []controller
}

type combinedListener struct {
	net.Listener
	locker *FileLocker // for unix domain socket
}

func (l *combinedListener) Close() error {
	if l.locker != nil {
		l.locker.Release()
		l.locker = nil
	}
	return l.Listener.Close()
}

type combinedUnixConn struct {
	*net.UnixConn
	path   string
	locker *FileLocker // for unix domain socket
}

func (l *combinedUnixConn) Close() error {
	if l.locker != nil {
		l.locker.Release()
		l.locker = nil
	}
	err := l.UnixConn.Close()
	if err != nil {
		return err
	}
	// Unlike UnixListener, Close() on UnixConn will not unlink the underlying file descriptor
	// We have to do it manually.
	return syscall.Unlink(l.path)
}

func getControlFunc(ctx context.Context, sockopt *SocketConfig, controllers []controller) func(network, address string, c syscall.RawConn) error {
	return func(network, address string, c syscall.RawConn) error {
		return c.Control(func(fd uintptr) {
			if sockopt != nil {
				if err := applyInboundSocketOptions(network, fd, sockopt); err != nil {
					newError("failed to apply socket options to incoming connection").Base(err).WriteToLog(session.ExportIDToError(ctx))
				}
			}

			setReusePort(fd) // nolint: staticcheck

			for _, controller := range controllers {
				if err := controller(network, address, fd); err != nil {
					newError("failed to apply external controller").Base(err).WriteToLog(session.ExportIDToError(ctx))
				}
			}
		})
	}
}

func (dl *DefaultListener) Listen(ctx context.Context, addr net.Addr, sockopt *SocketConfig) (net.Listener, error) {
	var lc net.ListenConfig
	var network, address string
	// callback is called after the Listen function returns
	// this is used to wrap the listener and do some post processing
	callback := func(l net.Listener, err error) (net.Listener, error) {
		return l, err
	}
	switch addr := addr.(type) {
	case *net.TCPAddr:
		network = addr.Network()
		if sockopt != nil && sockopt.ListenStrategy != SocketConfig_USE_IP {
			switch sockopt.ListenStrategy {
			case SocketConfig_USE_IP4:
				network = "tcp4"
			case SocketConfig_USE_IP6:
				network = "tcp6"
			}
		}
		address = addr.String()
		lc.Control = getControlFunc(ctx, sockopt, dl.controllers)
		if sockopt != nil && (sockopt.TcpKeepAliveInterval != 0 || sockopt.TcpKeepAliveIdle != 0) {
			lc.KeepAlive = time.Duration(-1)
		}
	case *net.UnixAddr:
		lc.Control = nil
		network = addr.Network()
		address = addr.Name
		if (runtime.GOOS == "linux" || runtime.GOOS == "android") && address[0] == '@' {
			// linux abstract unix domain socket is lockfree
			if len(address) > 1 && address[1] == '@' {
				// but may need padding to work with haproxy
				fullAddr := make([]byte, len(syscall.RawSockaddrUnix{}.Path))
				copy(fullAddr, address[1:])
				address = string(fullAddr)
			}
		} else {
			// normal unix domain socket
			var fileMode *os.FileMode
			// parse file mode from address
			if s := strings.Split(address, ","); len(s) == 2 {
				fMode, err := strconv.ParseUint(s[1], 8, 32)
				if err != nil {
					return nil, newError("failed to parse file mode").Base(err)
				}
				address = s[0]
				fm := os.FileMode(fMode)
				fileMode = &fm
			}
			// normal unix domain socket must be unlinked before listening
			if err := syscall.Unlink(address); err != nil && err != syscall.ENOENT {
				return nil, newError("failed to unlink unix socket file: ", address).Base(err)
			}
			// normal unix domain socket needs lock
			locker := &FileLocker{
				path: address + ".lock",
			}
			if err := locker.Acquire(); err != nil {
				return nil, err
			}
			// set file mode for unix domain socket when it is created
			callback = func(l net.Listener, err error) (net.Listener, error) {
				if err != nil {
					locker.Release()
					return nil, err
				}
				l = &combinedListener{Listener: l, locker: locker}
				if fileMode == nil {
					return l, err
				}
				if cerr := os.Chmod(address, *fileMode); cerr != nil {
					// failed to set file mode, close the listener
					l.Close()
					return nil, newError("failed to set file mode for file: ", address).Base(cerr)
				}
				return l, err
			}
		}
	}

	l, err := lc.Listen(ctx, network, address)
	l, err = callback(l, err)
	if err == nil && sockopt != nil && sockopt.AcceptProxyProtocol {
		policyFunc := func(upstream net.Addr) (proxyproto.Policy, error) { return proxyproto.REQUIRE, nil }
		l = &proxyproto.Listener{Listener: l, Policy: policyFunc}
	}
	return l, err
}

func (dl *DefaultListener) ListenPacket(ctx context.Context, addr net.Addr, sockopt *SocketConfig) (net.PacketConn, error) {
	var lc net.ListenConfig
	var network, address string

	// callback is called after the Listen function returns
	// this is used to wrap the listener and do some post processing
	callback := func(l net.PacketConn, err error) (net.PacketConn, error) {
		return l, err
	}
	switch addr := addr.(type) {
	case *net.UDPAddr:
		network = addr.Network()
		if sockopt != nil && sockopt.ListenStrategy != SocketConfig_USE_IP {
			switch sockopt.ListenStrategy {
			case SocketConfig_USE_IP4:
				network = "udp4"
			case SocketConfig_USE_IP6:
				network = "udp6"
			}
		}
		address = addr.String()
		lc.Control = getControlFunc(ctx, sockopt, dl.controllers)
	case *net.UnixAddr:
		lc.Control = nil
		network = addr.Network()
		address = addr.Name
		if (runtime.GOOS == "linux" || runtime.GOOS == "android") && address[0] == '@' {
			// linux abstract unix domain socket is lockfree
			if len(address) > 1 && address[1] == '@' {
				// but may need padding to work with haproxy
				fullAddr := make([]byte, len(syscall.RawSockaddrUnix{}.Path))
				copy(fullAddr, address[1:])
				address = string(fullAddr)
			}
		} else {
			// normal unix domain socket
			var fileMode *os.FileMode
			// parse file mode from address
			if s := strings.Split(address, ","); len(s) == 2 {
				fMode, err := strconv.ParseUint(s[1], 8, 32)
				if err != nil {
					return nil, newError("failed to parse file mode").Base(err)
				}
				address = s[0]
				fm := os.FileMode(fMode)
				fileMode = &fm
			}
			// normal unix domain socket must be unlinked before listening
			if err := syscall.Unlink(address); err != nil && err != syscall.ENOENT {
				return nil, newError("failed to unlink unix socket file: ", address).Base(err)
			}
			// normal unix domain socket needs lock
			locker := &FileLocker{
				path: address + ".lock",
			}
			if err := locker.Acquire(); err != nil {
				return nil, err
			}
			// set file mode for unix domain socket when it is created
			callback = func(l net.PacketConn, err error) (net.PacketConn, error) {
				if err != nil {
					locker.Release()
					return nil, err
				}
				l = &combinedUnixConn{UnixConn: l.(*net.UnixConn), path: address, locker: locker}
				if fileMode == nil {
					return l, err
				}
				if cerr := os.Chmod(address, *fileMode); cerr != nil {
					// failed to set file mode, close the listener
					l.Close()
					return nil, newError("failed to set file mode for file: ", address).Base(cerr)
				}
				return l, err
			}
		}
	}

	l, err := lc.ListenPacket(ctx, network, address)
	l, err = callback(l, err)
	return l, err
}

// RegisterListenerController adds a controller to the effective system listener.
// The controller can be used to operate on file descriptors before they are put into use.
//
// v2ray:api:beta
func RegisterListenerController(controller func(network, address string, fd uintptr) error) error {
	if controller == nil {
		return newError("nil listener controller")
	}

	effectiveListener.controllers = append(effectiveListener.controllers, controller)
	return nil
}

type SystemListener interface {
	Listen(ctx context.Context, addr net.Addr, sockopt *SocketConfig) (net.Listener, error)
	ListenPacket(ctx context.Context, addr net.Addr, sockopt *SocketConfig) (net.PacketConn, error)
}
