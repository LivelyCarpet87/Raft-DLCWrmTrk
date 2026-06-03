package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"strings"
	"time"
	"flag"
	"fmt"

	"raft-dlcwrmtrk/fsm"
	"raft-dlcwrmtrk/raftnode"
	"raft-dlcwrmtrk/raftcommands"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/go-hclog"

	_ "modernc.org/sqlite"
)

type JoinRequest struct {
	ID       string `json:"id"`
	FailureDomain string `json:"failureDomain"`
	RaftAddr string `json:"raft_addr"`
	GrpcAddr string `json:"grpc_addr"`
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
	rootLogger := hclog.New(&hclog.LoggerOptions{
		Name:  "cluster",
		Level: hclog.Debug,
	})
	log := rootLogger.Named("main")

	nodeID := 			flag.String("nodeID", "", "The ID for this node. Each node in the cluster must have a distinct ID.")
	failureDomain := 	flag.String("fd", "", "The failure domain for this node. Nodes with the same listed failure domain should be more likely to fail together.")
	ip_raft := 			flag.String("addr.raft", "0.0.0.0", "The IP address the raft node listens at.")
	ip_grpc := 			flag.String("addr.grpc", "", "The IP address the grpc server listens at. Defaults to the value for addr.raft")
	ip_http := 			flag.String("addr.http", "", "The IP address the http server listens at. Defaults to the value for addr.raft")
	port_raft := 		flag.Int("port.raft", 6000, "The RAFT port for this node. Used for RAFT coordination within the cluster. Defaults to 6000.")
	port_grpc := 		flag.Int("port.grpc", -1, "The gRPC port for this node. Used for gRPC calls within the cluster. Defaults to port.raft+1000.")
	port_http := 		flag.Int("port.http", -1, "The http port for this node. Used by the HTTP server for the web API. Defaults to port.raft+2000.")
	bootstrap :=		flag.Bool("bootstrap", false, "Bootstrap when starting the cluster for the first time. ONLY 1 node can be bootstrapped.")
	peersList :=		flag.String("peers", "", "A comma separated list of peers in the cluster. Ex. addr1:grpc_port1,addr2:grpc_port2")

	flag.Parse()
	if (*nodeID == ""){
		flag.Usage()
		panic("nodeID is required.")
	} else if (*failureDomain == ""){
		flag.Usage()
		panic("failure domain is required.")
	} else if (*port_raft < 1 || *port_raft > 65535){
		flag.Usage()
		panic("port.raft must be between 1 and 65535.")
	} else if (!*bootstrap && *peersList == ""){
		flag.Usage()
		panic("A peer list must be provided when not bootstrapping.")
	}

	if (*ip_grpc == ""){
		ip_grpc = ip_raft
		log.Info("Using default value", "gRPC_IP",*ip_grpc,)
	}
	if (*ip_http == ""){
		ip_http = ip_raft
		log.Info("Using default value", "HTTP_IP",*ip_http)
	}

	log.Info("Using","RAFT port",*port_raft,)
	if (*port_grpc == -1){
		*port_grpc = *port_raft + 1000
		log.Info("Using default value","gRPC_port",*port_grpc)
	}
	if (*port_grpc < 1 || *port_grpc > 65535){
		flag.Usage()
		panic("port.grpc must be between 1 and 65535.")
	}

	if (*port_http == -1){
		*port_http = *port_raft + 2000
		log.Info("Using default value","HTTP_port",*port_http)
	}
	if (*port_http < 1 || *port_http > 65535){
		flag.Usage()
		panic("port.http must be between 1 and 65535.")
	}

	leader := ""
	peers := strings.Split(*peersList, ",")
	raftAddr := fmt.Sprintf("%s:%d", *ip_raft, *port_raft)
	grpcAddr := fmt.Sprintf("%s:%d", *ip_grpc, *port_grpc)
	httpAddr := fmt.Sprintf("%s:%d", *ip_http, *port_http)


	if (!*bootstrap) {
		leader_found, err := discoverLeader(peers)
		if err != nil {
			log.Error("Failed to find leader", "err",err)
		}
		leader = leader_found
		log.Info("Found leader", "leader",leader)
	}

	db, _ := sql.Open("sqlite", *nodeID+".db")
	fsm.InitSchema(db)

	node, err := raftnode.NewNode(*nodeID, raftAddr, *failureDomain, grpcAddr, httpAddr, db, rootLogger, *bootstrap)
	if err != nil {
		log.Error("Failed to create RAFT node", "err",err)
	}

	if !*bootstrap {
		time.Sleep(500 * time.Millisecond) // small safety delay
	
		selfJoinReq := JoinRequest{
			ID:       *nodeID,
			FailureDomain: *failureDomain,
			RaftAddr: raftAddr,
			GrpcAddr: grpcAddr,
			HttpAddr: httpAddr,
		}
		if err := joinCluster(leader, selfJoinReq); err != nil {
			log.Error("Failed to join cluster", "err",err)
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

	// ---- CLUSTER JOIN ----
	http.HandleFunc("/cluster/join", func(w http.ResponseWriter, r *http.Request) {

		if node.Raft.State() != raft.Leader {
			http.Error(w, "not leader", 403)
			return
		}

		var req JoinRequest
		json.NewDecoder(r.Body).Decode(&req)

		addNodeCommand := raftcommands.AddNodeCommand{
			NodeUID: req.ID,
			FailureDomain: req.FailureDomain,
			RaftAddr: req.RaftAddr,
			GrpcAddr: req.GrpcAddr,
			HttpAddr: req.HttpAddr,
		}
		cmdData, _ := json.Marshal(addNodeCommand)

		cmdEnv := raftcommands.CommandEnvelope{
			Command: "AddNode",
			Data: cmdData,
		}

		cmdEnvData, _ := json.Marshal(cmdEnv)

		if err := node.Raft.Apply(cmdEnvData, 5*time.Second).Error(); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		if err := node.AddRaftNode(req.ID, req.RaftAddr); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		w.Write([]byte("joined"))
	})

	log.Info("Starting HTTP server", "httpAddr",httpAddr)
	go http.ListenAndServe(httpAddr, nil)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	<-sig
}