package main

import (
	"bytes"
	"os"
    "path/filepath"
	"text/template"

	"github.com/docker/go-units"
	"gopkg.in/yaml.v3"
)

type ConfigYaml struct {
	BasePath string `yaml:"base_path"`
	NodeID string `yaml:"node_id"`
	FailureDomain string `yaml:"failure_domain"`
	RaftBindAddr string `yaml:"raft_bind_addr"`
	HttpBindAddr string `yaml:"http_bind_addr"`

	Storage struct {
		NumVNodes int `yaml:"num_vnodes"`
		MaxStorage string  `yaml:"max_storage"`
	} `yaml:"storage"`
}

type Config struct {
	BasePath string
	NodeID string 
	FailureDomain string
	RaftBindAddr string 
	HttpBindAddr string 

	Storage struct {
		NumVNodes int
		MaxStorage int64
	}
}

const configTemplate = `
# Root directory containing all node data.
base_path: {{ .BasePath }}

# Unique node identity.
# This is set for when the node is first created.
# Do not edit after the node has joined a cluster.
# Each instance should have a unique NodeID
node_id: NODE_ID_CHANGE_ME

# Failure domain.
# This should be an identifier for a physical or 
# logical section of the computing environment
# that shares a single point of failure.
# The system will try to spread data across
# different failure domains to improve redundancy.
# Common examples are server racks, rooms, or regions.
# See: https://www.digital-infrastructure-explained.com/what-failure-domains-mean.html
failure_domain: CHANGE_ME_FAILURE_DOMAIN_Room_A113

# Address this node listens on for RAFT.
# This address must be advertiseable
raft_bind_addr: 127.0.0.1:7000

# Address this node listens on for HTTP.
http_bind_addr: 0.0.0.0:8000

storage:
  # Number of vNodes to allocate on this node.
  # Changing this number to be smaller this across multiple nodes may cause file loss.
  num_vnodes: 4
  
  # Maximum bytes that EACH vNode may store. 
  # Make sure not to exceed total available space.
  max_storage: 20GiB
`

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfgYml ConfigYaml

	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)

	if err := dec.Decode(&cfgYml); err != nil {
		return nil, err
	}
	maxStorage, storageSizeErr := units.RAMInBytes(cfgYml.Storage.MaxStorage)
	if storageSizeErr != nil {
		return nil, storageSizeErr
	}
	cfg := Config{
		BasePath: cfgYml.BasePath,
		NodeID: cfgYml.NodeID,
		FailureDomain: cfgYml.FailureDomain,
		RaftBindAddr: cfgYml.RaftBindAddr,
		HttpBindAddr: cfgYml.HttpBindAddr,
		Storage: struct {
			NumVNodes int
			MaxStorage int64
		}{
			NumVNodes: cfgYml.Storage.NumVNodes,
			MaxStorage: maxStorage,
		},
	}

	return &cfg, nil
}

func WriteConfig(path string) error {
	tmpl, err := template.New("config").Parse(configTemplate)
	if err != nil {
		return err
	}
	defaultBasePath := filepath.Dir(path)

	var buf bytes.Buffer
	type CfgTemplateInfo struct {
		BasePath string
	}

	if err := tmpl.Execute(&buf, CfgTemplateInfo{BasePath: defaultBasePath}); err != nil {
		return err
	}

	return os.WriteFile(path, buf.Bytes(), 0644)
}

