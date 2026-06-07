package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/signal"
	"syscall"
	"strings"
	"time"
	"flag"
	"fmt"
	"net/http"
	"net/url"

	"raft-dlcwrmtrk/fsm"
	"raft-dlcwrmtrk/raftnode"
	// "raft-dlcwrmtrk/raftcommands"
	"raft-dlcwrmtrk/httpserver/server"
	rt "raft-dlcwrmtrk/httpserver/responsetypes"

	"github.com/hashicorp/go-hclog"

	_ "modernc.org/sqlite"
)

func discoverLeader(seeds []string) (string, error) {
	for _, s := range seeds {
		resp, err := http.Get("http://" + s + "/raft/leader")
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		var response rt.Response[rt.LeaderHttpAddrResponse]
		body, _ := io.ReadAll(resp.Body)
		if (json.Unmarshal(body, &response) != nil) {
			panic(err)
		}
		if (response.Success){
			return response.Data.Leader, nil
		}
	}
	return "", errors.New("no leader found")
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
	ip_http := 			flag.String("addr.http", "", "The IP address the http server listens at. Defaults to the value for addr.raft")
	port_raft := 		flag.Int("port.raft", 6000, "The RAFT port for this node. Used for RAFT coordination within the cluster. Defaults to 6000.")
	port_http := 		flag.Int("port.http", -1, "The http port for this node. Used by the HTTP server for the web API. Defaults to port.raft+2000.")
	bootstrap :=		flag.Bool("bootstrap", false, "Bootstrap when starting the cluster for the first time. ONLY 1 node can be bootstrapped.")
	peersList :=		flag.String("peers", "", "A comma separated list of peers in the cluster. Ex. addr1:http_port1,addr2:http_port2")

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

	if (*ip_http == ""){
		ip_http = ip_raft
		log.Info("Using default value", "HTTP_IP",*ip_http)
	}

	log.Info("Using","RAFT port",*port_raft,)

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

	node, err := raftnode.NewNode(*nodeID, raftAddr, *failureDomain, httpAddr, db, rootLogger, *bootstrap)
	if err != nil {
		log.Error("Failed to create RAFT node", "err",err)
	}

	if !*bootstrap {
		time.Sleep(500 * time.Millisecond) // small safety delay
	
		form := url.Values{}
		form.Add("nodeID", *nodeID)
		form.Add("raftAddr", raftAddr)
		form.Add("failureDomain", *failureDomain)
		form.Add("httpAddr", httpAddr)

		resp, err := http.PostForm(
			"http://"+leader+"/raft/join",
			form,
		)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()

		_, err = io.ReadAll(resp.Body)
		if err != nil {
			panic(err)
		}
	}

	

	log.Info("Starting HTTP server", "httpAddr",httpAddr)
	httpLogger := rootLogger.Named("httpServer")
	s := server.New(node, httpLogger)
	s.Run(httpAddr)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	<-sig
}