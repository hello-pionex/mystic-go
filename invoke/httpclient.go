package invoke

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/rs/dnscache"
)

func newDialContextWithCachedResolver(dialer *net.Dialer, resolver *dnscache.Resolver) func(ctx context.Context, network string, address string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}

		ips, err := resolver.LookupHost(ctx, host)
		if err != nil {
			return nil, err
		}

		for _, ip := range ips {
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip, port))
			if err == nil {
				return conn, nil
			}
		}

		// 这里做容错
		return dialer.DialContext(ctx, network, address)
	}
}

var (
	DefaultHttpClient = &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: newDialContextWithCachedResolver(&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				Resolver:  nil,
			}, &dnscache.Resolver{
				Timeout: time.Second * 5,
			}),
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
)
