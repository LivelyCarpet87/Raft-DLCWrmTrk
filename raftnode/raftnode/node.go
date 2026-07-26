package raftnode

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	bolt "github.com/hashicorp/raft-boltdb"

	"raft-dlcwrmtrk/fsm"
	"raft-dlcwrmtrk/httpserver/responsetypes"
	"raft-dlcwrmtrk/raftcommands"
)

type Node struct {
	NodeID string
	Raft   *raft.Raft
	FSM    *fsm.FSM
}

func NewNode(basePath string, id string, raftAddr string, failureDomain string, httpAddr string, rootLogger hclog.Logger, bootstrap bool) (*Node, error) {

	raftLogger := rootLogger.Named("raft")
	fsmLogger := rootLogger.Named("fsm")

	raftPath := filepath.Join(basePath, "raft")
	fsmPath := filepath.Join(basePath, "fsm")

	dbPath := filepath.Join(fsmPath, "fsm.db")
	db, _ := sql.Open("sqlite", dbPath)
	fsm.InitSchema(db)
	f := fsm.New(db, fsmPath, fsmLogger)

	logStore, _ := bolt.NewBoltStore(filepath.Join(raftPath, "log.bolt"))
	stableStore, _ := bolt.NewBoltStore(filepath.Join(raftPath, "stable.bolt"))

	snapshots, err := raft.NewFileSnapshotStore(filepath.Join(raftPath, "data"), 2, os.Stdout)
	if err != nil {
		return nil, err
	}

	addr, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		raftLogger.Error("failed to bind to address", "raftAddr", raftAddr, "err", err)
		return nil, err
	}
	transport, err := raft.NewTCPTransport(raftAddr, addr, 3, 10*time.Second, os.Stdout)
	if err != nil {
		raftLogger.Error("failed to create new TCP transport", "raftAddr", raftAddr, "err", err)
		return nil, err
	}

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(id)
	config.Logger = raftLogger

	r, err := raft.NewRaft(config, f, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, err
	}

	if bootstrap {
		config := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(id),
					Address: raft.ServerAddress(raftAddr),
				},
			},
		}

		f := r.BootstrapCluster(config)
		if err := f.Error(); err != nil {
			return nil, err
		}
		for r.State() != raft.Leader {
			time.Sleep(50 * time.Millisecond)
		}
		addNodeCommand := raftcommands.AddNodeCommand{
			NodeID:        id,
			FailureDomain: failureDomain,
			RaftAddr:      raftAddr,
			HttpAddr:      httpAddr,
		}
		cmdData, _ := json.Marshal(addNodeCommand)

		cmdEnv := raftcommands.CommandEnvelope{
			Command: "AddNode",
			Data:    cmdData,
		}
		cmdEnvData, _ := json.Marshal(cmdEnv)

		r.Apply(cmdEnvData, 5*time.Second)
	}

	return &Node{
		NodeID: id,
		Raft:   r,
		FSM:    f,
	}, nil
}

func (n *Node) ProxyApply(cmdEnv raftcommands.CommandEnvelope) error {
	cmdEnvData, _ := json.Marshal(cmdEnv)

	if n.IsLeader() {
		return n.Raft.Apply(cmdEnvData, 5*time.Second).Error()
	} else {
		leader, err := n.GetLeaderHttpAddr()
		if err != nil {
			return err
		}

		resp, httpErr := http.Post(
			"http://"+leader+"/raft/apply",
			"application/octet-stream",
			bytes.NewReader(cmdEnvData),
		)

		if httpErr != nil {
			return httpErr
		}
		defer resp.Body.Close()

		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return readErr
		}

		var response responsetypes.Response[any]
		if json.Unmarshal(body, &response) != nil {
			panic(err)
		}
		if !response.Success {
			return errors.New(response.Error.Message)
		}

	}
	return nil
}

func (n *Node) GetReadOnlyTx(ctx context.Context) (*sql.Tx, error) {
	return n.FSM.GetReadOnlyTx(ctx)
}

func (n *Node) AddRaftNode(nodeID string, failureDomain string, raftAddr string, httpAddr string) error {
	addNodeCommand := raftcommands.AddNodeCommand{
		NodeID:        nodeID,
		FailureDomain: failureDomain,
		RaftAddr:      raftAddr,
		HttpAddr:      httpAddr,
	}
	cmdData, _ := json.Marshal(addNodeCommand)
	cmdEnv := raftcommands.CommandEnvelope{
		Command: "AddNode",
		Data:    cmdData,
	}
	cmdEnvData, _ := json.Marshal(cmdEnv)
	applyFuture := n.Raft.Apply(cmdEnvData, 5*time.Second) // TODO: Detect and fix if metadata applied but voter not added
	if err := applyFuture.Error(); err != nil {
		return err
	}
	return n.Raft.AddVoter(
		raft.ServerID(nodeID),
		raft.ServerAddress(raftAddr),
		0,
		0,
	).Error()
}

func (n *Node) RemoveRaftNode(id string) error {
	return n.Raft.RemoveServer(
		raft.ServerID(id),
		0,
		0,
	).Error()
}

func (n *Node) IsLeader() bool {
	return n.Raft.State() == raft.Leader
}

func (n *Node) GetLeaderHttpAddr() (string, error) {
	leaderRaftAddr, _ := n.Raft.LeaderWithID()
	if leaderRaftAddr == "" {
		return "", errors.New("no leader")
	}
	return n.FSM.QueryHttpAddrFromRaftAddr(string(leaderRaftAddr))
}

func (n *Node) GetRaftNodeID() string {
	return n.NodeID
}
