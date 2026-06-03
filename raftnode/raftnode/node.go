package raftnode

import (
	"database/sql"
	"net"
	"os"
	"time"
	"encoding/json"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/go-hclog"
	bolt "github.com/hashicorp/raft-boltdb"

	"raft-dlcwrmtrk/fsm"
)

type Node struct {
	Raft *raft.Raft
	FSM  *fsm.FSM
	DB   *sql.DB
}

type Command struct {
	Op   string        `json:"op"`
	Args []interface{} `json:"args"`
}

type Envelope struct {
	Cmd Command `json:"cmd"`
}

func NewNode(id, raftAddr string, httpAddr string, db *sql.DB,  rootLogger hclog.Logger, bootstrap bool) (*Node, error) {

	raftLogger := rootLogger.Named("raft")
	fsmLogger  := rootLogger.Named("fsm")

	f := fsm.New(db, "./", fsmLogger)

	logStore, _ := bolt.NewBoltStore(id + "-log.bolt")
	stableStore, _ := bolt.NewBoltStore(id + "-stable.bolt")

	snapshots, err := raft.NewFileSnapshotStore("data/"+id, 2, os.Stdout)
	if err != nil {
		return nil, err
	}

	addr, _ := net.ResolveTCPAddr("tcp", raftAddr)
	transport, _ := raft.NewTCPTransport(raftAddr, addr, 3, 10*time.Second, os.Stdout)

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(id)
	config.Logger = raftLogger

	r, err := raft.NewRaft(config, f, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, err
	}

	// ---- BOOTSTRAP SAFETY ----
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
		cmd := Envelope{
			Cmd: Command{
				Op: "add_node",
				Args: []interface{}{
					id,
					raftAddr,
					httpAddr,
				},
			},
		}
		data, _ := json.Marshal(cmd)
		r.Apply(data, 5*time.Second)
	}

	return &Node{
		Raft: r,
		FSM:  f,
		DB:   db,
	}, nil
}

func (n *Node) AddRaftNode(id, addr string) error {
	return n.Raft.AddVoter(
		raft.ServerID(id),
		raft.ServerAddress(addr),
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