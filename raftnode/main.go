package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"raft-dlcwrmtrk/fsm"
	"raft-dlcwrmtrk/raftnode"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/go-hclog"

	_ "modernc.org/sqlite"
)

type Command struct {
	Op   string        `json:"op"`
	Args []interface{} `json:"args"`
}

type Envelope struct {
	Cmd Command `json:"cmd"`
}

type JoinRequest struct {
	ID       string `json:"id"`
	RaftAddr string `json:"raft_addr"`
	HttpAddr string `json:"http_addr"`
}

func discoverLeader(seeds []string) (string, error) {
	for _, s := range seeds {
		resp, err := http.Get("http://" + s + "/leader")
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		b, _ := io.ReadAll(resp.Body)
		leader := string(b)

		if resp.StatusCode == 200 {
			return leader, nil
		}
	}
	return "", errors.New("no leader found")
}

func joinCluster(leader string, self JoinRequest) error {

	data, _ := json.Marshal(self)

	resp, err := http.Post(
		"http://"+leader+"/cluster/join",
		"application/json",
		bytes.NewReader(data),
	)

	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return errors.New(string(body))
	}

	return nil
}

func main() {

	id := os.Args[1]
	raftAddr := os.Args[2]
	httpAddr := os.Args[3]
	bootstrap := false
	leader := ""

	rootLogger := hclog.New(&hclog.LoggerOptions{
		Name:  "cluster",
		Level: hclog.Debug,
	})

	if (os.Args[4] == "bootstrap") {
		bootstrap = true
	} else if (os.Args[4] == "join") {
		leader = os.Args[5]
	} else {
		seeds := strings.Split(os.Args[4], ",")
		leader_found, err := discoverLeader(seeds)
		if err != nil {
			log.Fatal(err)
		}
		leader = leader_found
	}

	db, _ := sql.Open("sqlite", id+".db")
	fsm.InitSchema(db)

	node, err := raftnode.NewNode(id, raftAddr, httpAddr, db, rootLogger, bootstrap)
	if err != nil {
		log.Fatal(err)
	}

	if !bootstrap {
		time.Sleep(500 * time.Millisecond) // small safety delay
	
		selfJoinReq := JoinRequest{
			ID:       id,
			RaftAddr: raftAddr,
			HttpAddr: httpAddr,
		}
		if err := joinCluster(leader, selfJoinReq); err != nil {
			log.Fatal(err)
		}
	}

	// ---- LEADER INFO ----
	http.HandleFunc("/leader", func(w http.ResponseWriter, r *http.Request) {
		leaderRaft := node.Raft.Leader()

		if leaderRaft == "" {
			http.Error(w, "no leader", http.StatusServiceUnavailable)
			return
		}

		httpAddr,err := node.FSM.QueryHttpAddrFromRaftAddr(string(leaderRaft))

		if err != nil {
			http.Error(w, "leader mapping not found", http.StatusInternalServerError)
			return
		}
		w.Write([]byte(httpAddr))
	})

	// ---- EXEC (KV ops) ----
	http.HandleFunc("/exec", func(w http.ResponseWriter, r *http.Request) {

		if node.Raft.State() != raft.Leader {
			http.Error(w, "not leader", 403)
			return
		}

		body, _ := io.ReadAll(r.Body)

		f := node.Raft.Apply(body, 5*time.Second)
		if err := f.Error(); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		w.Write([]byte("ok"))
	})

	// ---- CLUSTER JOIN ----
	http.HandleFunc("/cluster/join", func(w http.ResponseWriter, r *http.Request) {

		if node.Raft.State() != raft.Leader {
			http.Error(w, "not leader", 403)
			return
		}

		var req JoinRequest
		json.NewDecoder(r.Body).Decode(&req)

		cmd := Envelope{
			Cmd: Command{
				Op: "add_node",
				Args: []interface{}{
					req.ID,
					req.RaftAddr,
					req.HttpAddr,
				},
			},
		}

		data, _ := json.Marshal(cmd)

		if err := node.Raft.Apply(data, 5*time.Second).Error(); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		if err := node.AddRaftNode(req.ID, req.RaftAddr); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		w.Write([]byte("joined"))
	})

	log.Println("http on", httpAddr)
	log.Fatal(http.ListenAndServe(httpAddr, nil))
}