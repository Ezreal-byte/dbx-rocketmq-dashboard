package main

import (
	"context"
	"fmt"
	"time"

	admin "github.com/amigoer/rocketmq-admin-go"
)

const (
	defaultRequestTimeout          = 30 * time.Second
	defaultConnectTimeout          = 10 * time.Second
	brokerRegistrationPollInterval = 100 * time.Millisecond
)

type socksProxyConfig struct {
	Host     string
	Port     int
	Username string
	Password string
}

type connectionConfig struct {
	UseTLS         bool
	VIPChannel     bool
	ProxyAddr      string
	NameServers    []string
	ClusterName    string
	BrokerAddr     string
	AccessKey      string
	SecretKey      string
	RequestTimeout time.Duration
	ConnectTimeout time.Duration
	TLSSkipVerify  bool
	SocksProxy     *socksProxyConfig
}

// buildClient opens a NameServer client and waits until at least one master
// broker has registered, so `connect` fails fast with a readable error instead
// of returning a half-usable connection.
func buildClient(config connectionConfig) (*admin.Client, *proxyManager, *admin.ClusterInfo, error) {
	proxies := newProxyManager(config)
	localNameServers := make([]string, 0, len(config.NameServers))
	for _, address := range config.NameServers {
		local, err := proxies.ProxyFor(address, proxyTargetNameServer)
		if err != nil {
			proxies.Close()
			return nil, nil, nil, err
		}
		localNameServers = append(localNameServers, local)
	}
	client, err := admin.NewClient(
		admin.WithNameServers(localNameServers),
		admin.WithTimeout(config.RequestTimeout),
		admin.WithRetryTimes(1),
	)
	if err != nil {
		proxies.Close()
		return nil, nil, nil, err
	}
	if err := client.Start(); err != nil {
		proxies.Close()
		return nil, nil, nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), config.ConnectTimeout)
	defer cancel()
	clusterInfo, err := waitForBrokerRegistration(ctx, brokerRegistrationPollInterval, client.ExamineBrokerClusterInfo)
	if err != nil {
		client.Close()
		proxies.Close()
		return nil, nil, nil, err
	}
	return client, proxies, clusterInfo, nil
}

func waitForBrokerRegistration(
	ctx context.Context,
	pollInterval time.Duration,
	examine func(context.Context) (*admin.ClusterInfo, error),
) (*admin.ClusterInfo, error) {
	if pollInterval <= 0 {
		pollInterval = brokerRegistrationPollInterval
	}
	var lastErr error
	for {
		clusterInfo, err := examine(ctx)
		if err == nil && hasMasterBroker(clusterInfo) {
			return clusterInfo, nil
		}
		if err != nil {
			lastErr = err
		}

		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			if lastErr != nil {
				return nil, fmt.Errorf("wait for RocketMQ broker registration after %v: %w", lastErr, ctx.Err())
			}
			return nil, fmt.Errorf("wait for RocketMQ broker registration: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

func hasMasterBroker(info *admin.ClusterInfo) bool {
	if info == nil {
		return false
	}
	for _, broker := range info.BrokerAddrTable {
		if broker != nil && broker.BrokerAddrs["0"] != "" {
			return true
		}
	}
	return false
}

func resolveClusterName(info *admin.ClusterInfo, configured string) string {
	if configured != "" {
		return configured
	}
	for _, name := range sortedKeys(info.ClusterAddrTable) {
		return name
	}
	return "DefaultCluster"
}

func resolveBrokerAddr(info *admin.ClusterInfo, config connectionConfig, proxies *proxyManager) (string, error) {
	if config.BrokerAddr != "" {
		if local := proxies.LocalForOriginal(config.BrokerAddr); local != "" {
			return local, nil
		}
		return proxies.ProxyFor(config.BrokerAddr, proxyTargetBroker)
	}
	for _, brokerName := range sortedKeys(info.BrokerAddrTable) {
		broker := info.BrokerAddrTable[brokerName]
		if address := broker.BrokerAddrs["0"]; address != "" {
			return address, nil
		}
		for _, brokerID := range sortedKeys(broker.BrokerAddrs) {
			if broker.BrokerAddrs[brokerID] != "" {
				return broker.BrokerAddrs[brokerID], nil
			}
		}
	}
	return "", fmt.Errorf("no RocketMQ broker address found")
}

func minDuration(left, right time.Duration) time.Duration {
	if left <= 0 || left > right {
		return right
	}
	return left
}

func (config connectionConfig) equal(other connectionConfig) bool {
	if config.ClusterName != other.ClusterName || config.BrokerAddr != other.BrokerAddr ||
		config.AccessKey != other.AccessKey || config.SecretKey != other.SecretKey ||
		config.RequestTimeout != other.RequestTimeout || config.ConnectTimeout != other.ConnectTimeout ||
		config.TLSSkipVerify != other.TLSSkipVerify ||
		len(config.NameServers) != len(other.NameServers) {
		return false
	}
	for index := range config.NameServers {
		if config.NameServers[index] != other.NameServers[index] {
			return false
		}
	}
	if (config.SocksProxy == nil) != (other.SocksProxy == nil) {
		return false
	}
	return config.SocksProxy == nil || *config.SocksProxy == *other.SocksProxy
}
