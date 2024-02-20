package grpc

import (
	"context"
	"crypto/tls"

	"github.com/v2fly/v2ray-core/v5/common/net"
	"github.com/v2fly/v2ray-core/v5/transport/internet"
	"github.com/v2fly/v2ray-core/v5/transport/internet/security"
	"github.com/v2fly/v2ray-core/v5/transport/internet/tls/utls"
	"google.golang.org/grpc/credentials"
)

type securityEngineCreds struct {
	securityEngine    security.Engine
	serverDestination net.Destination

	streamSettings *internet.MemoryStreamConfig
	ctx            context.Context
}

func newSecurityEngineCreds(ctx context.Context, streamSettings *internet.MemoryStreamConfig) (credentials.TransportCredentials, error) {
	securityEngine, err := security.CreateSecurityEngineFromSettings(ctx, streamSettings)
	if err != nil {
		return nil, newError("unable to create security engine").Base(err)
	}
	var serverDestination net.Destination
	if engine, ok := securityEngine.(*utls.Engine); ok {
		serverDestination = net.TCPDestination(net.DomainAddress(engine.GetServerName()), net.Port(0))
	}
	return &securityEngineCreds{
		securityEngine:    securityEngine,
		serverDestination: serverDestination,

		streamSettings: streamSettings,
		ctx:            ctx,
	}, nil
}

// Info implements credentials.TransportCredentials.
func (c securityEngineCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{
		SecurityProtocol: "tls",
		SecurityVersion:  "1.2",
		ServerName:       c.serverDestination.Address.Domain(),
	}
}

// ClientHandshake implements credentials.TransportCredentials.
func (c *securityEngineCreds) ClientHandshake(ctx context.Context, authority string, rawConn net.Conn) (_ net.Conn, _ credentials.AuthInfo, err error) {
	if !c.serverDestination.IsValid() {
		serverName, serverPort, err := net.SplitHostPort(authority)
		if err != nil {
			// If the authority had no host port or if the authority cannot be parsed, use it as-is.
			serverName = authority
		}
		port, _ := net.PortFromString(serverPort)
		switch rawConn.LocalAddr().Network() {
		case "tcp", "tcp4", "tcp6":
			c.serverDestination = net.TCPDestination(net.DomainAddress(serverName), port)
		case "udp", "udp4", "udp6":
			c.serverDestination = net.UDPDestination(net.DomainAddress(serverName), port)
		case "unix":
			c.serverDestination = net.UnixDestination(net.DomainAddress(serverName))
		}
	}
	var conn security.Conn
	errChannel := make(chan error, 1)
	go func() {
		var e error
		conn, e = c.securityEngine.Client(rawConn, security.OptionWithDestination{Dest: c.serverDestination})
		errChannel <- e
		close(errChannel)
	}()
	select {
	case err := <-errChannel:
		if err != nil {
			conn.Close()
			return nil, nil, err
		}
	case <-ctx.Done():
		conn.Close()
		return nil, nil, ctx.Err()
	}
	authInfo := securityEngineAuthInfo{
		CommonAuthInfo: credentials.CommonAuthInfo{
			SecurityLevel: credentials.PrivacyAndIntegrity,
		},
		conn: conn,
	}
	return conn, authInfo, nil
}

// ServerHandshake implements credentials.TransportCredentials.
func (c *securityEngineCreds) ServerHandshake(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, newError("ServerHandshake is not implemented due to not Server() method in security.Engine")
}

// Clone implements credentials.TransportCredentials.
func (c *securityEngineCreds) Clone() credentials.TransportCredentials {
	creds, err := newSecurityEngineCreds(c.ctx, c.streamSettings)
	if err != nil {
		panic(err)
	}
	return creds
}

// OverrideServerName implements credentials.TransportCredentials.
func (c *securityEngineCreds) OverrideServerName(serverNameOverride string) error {
	c.serverDestination.Address = net.ParseAddress(serverNameOverride)
	return nil
}

// TLSInfo contains the auth information for a TLS authenticated connection.
// It implements the AuthInfo interface.
type securityEngineAuthInfo struct {
	credentials.CommonAuthInfo
	conn security.Conn
}

// AuthType returns the type of TLSInfo as a string.
func (t securityEngineAuthInfo) AuthType() string {
	return "tls"
}

// GetSecurityValue returns security info requested by channelz.
func (t securityEngineAuthInfo) GetSecurityValue() credentials.ChannelzSecurityValue {
	switch conn := t.conn.(type) {
	case utls.UTLSClientConnection:
		state := conn.UConn.ConnectionState()
		v := &credentials.TLSChannelzSecurityValue{
			StandardName: cipherSuiteLookup[state.CipherSuite],
		}
		// Currently there's no way to get LocalCertificate info from tls package.
		if len(state.PeerCertificates) > 0 {
			v.RemoteCertificate = state.PeerCertificates[0].Raw
		}
		return v
	default:
		return nil
	}
}

var cipherSuiteLookup = map[uint16]string{
	tls.TLS_RSA_WITH_RC4_128_SHA:                "TLS_RSA_WITH_RC4_128_SHA",
	tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA:           "TLS_RSA_WITH_3DES_EDE_CBC_SHA",
	tls.TLS_RSA_WITH_AES_128_CBC_SHA:            "TLS_RSA_WITH_AES_128_CBC_SHA",
	tls.TLS_RSA_WITH_AES_256_CBC_SHA:            "TLS_RSA_WITH_AES_256_CBC_SHA",
	tls.TLS_RSA_WITH_AES_128_GCM_SHA256:         "TLS_RSA_WITH_AES_128_GCM_SHA256",
	tls.TLS_RSA_WITH_AES_256_GCM_SHA384:         "TLS_RSA_WITH_AES_256_GCM_SHA384",
	tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA:        "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA",
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA:    "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA:    "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
	tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA:          "TLS_ECDHE_RSA_WITH_RC4_128_SHA",
	tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA:     "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA",
	tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA:      "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
	tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA:      "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:   "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256: "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384:   "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384: "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
	tls.TLS_FALLBACK_SCSV:                       "TLS_FALLBACK_SCSV",
	tls.TLS_RSA_WITH_AES_128_CBC_SHA256:         "TLS_RSA_WITH_AES_128_CBC_SHA256",
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256: "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
	tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256:   "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
	tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305:    "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305",
	tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305:  "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305",
	tls.TLS_AES_128_GCM_SHA256:                  "TLS_AES_128_GCM_SHA256",
	tls.TLS_AES_256_GCM_SHA384:                  "TLS_AES_256_GCM_SHA384",
	tls.TLS_CHACHA20_POLY1305_SHA256:            "TLS_CHACHA20_POLY1305_SHA256",
}
