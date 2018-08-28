package config

import (
	"strings"
	"strconv"
	"fmt"
	"unsafe"
)

type RoleType int

const (
	RoleTypeNameServer RoleType = iota
	RoleTypeDataServer
)

type Config struct {
	Role string
	// hostname1:port1;hostname2:port2;hostname3:port3
	NameServers string
	DataPath    string
}

type ParsedConfig struct {
	Role        RoleType
	DataPath    string
	NameServers []*ServerConfig
}

type ServerConfig struct {
	Hostname string
	Port     uint16
}

func (config *Config) Parse() (*ParsedConfig, error) {
	parsedConfig := new(ParsedConfig)
	switch config.Role {
	case "nameserver", "ns", "nameServer", "NameServer", "name":
		parsedConfig.Role = RoleTypeNameServer
	case "dataserver", "data", "dataServer", "DataServer", "node":
		parsedConfig.Role = RoleTypeDataServer
	default:
		return nil, fmt.Errorf("unknown role type: %s", config.Role)
	}

	parsedConfig.DataPath = config.DataPath

	parts := strings.Split(config.NameServers, ";")
	parsedConfig.NameServers = make([]*ServerConfig, 0, len(parts))
	for _, part := range parts {
		host := strings.Split(part, ":")
		if len(host) < 1 {
			return nil, fmt.Errorf("%s is not an hostname:port format", part)
		}
		hostname := host[0]
		port, err := strconv.Atoi(host[1])
		if err != nil {
			return nil, err
		}
		serverConfig := new(ServerConfig)
		serverConfig.Hostname = hostname
		serverConfig.Port = uint16(port)
		parsedConfig.NameServers = append(parsedConfig.NameServers, serverConfig)
	}
	return parsedConfig, nil
}
