package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	rt "raft-dlcwrmtrk/httpserver/responsetypes"
	"raft-dlcwrmtrk/httpserver/server"
	"raft-dlcwrmtrk/raftnode"
	"raft-dlcwrmtrk/vnode"
	"raft-dlcwrmtrk/workers"

	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"

	_ "modernc.org/sqlite"
)

func discoverLeader(seeds []string) string {
	leaderHttpAddr := ""
	for len(leaderHttpAddr) == 0 {
		time.Sleep(5 * time.Second)
		for _, s := range seeds {
			resp, err := http.Get(s + "/raft/leader")
			if err != nil {
				continue
			}
			defer resp.Body.Close()

			var response rt.Response[rt.LeaderHttpAddrResponse]
			body, _ := io.ReadAll(resp.Body)
			if json.Unmarshal(body, &response) != nil {
				continue
			}
			if response.Success {
				leaderHttpAddr = response.Data.Leader
			}
		}
	}
	return leaderHttpAddr
}

func main() {
	rootLogger := hclog.New(&hclog.LoggerOptions{
		Name:  "cluster",
		Level: hclog.Debug,
	})
	log := rootLogger.Named("main")

	cfgPath := flag.String("config", "", "The path of the config file.")
	genConfig := flag.Bool("generate-cfg", false, "Generate a default configuration file from template.")
	bootstrap := flag.Bool("bootstrap", false, "Bootstrap when starting the cluster for the first time. ONLY 1 node can be bootstrapped.")
	peersList := flag.String("peers", "", "A comma separated list of peers in the cluster. Ex. addr1:http_port1,addr2:http_port2")

	flag.Parse()

	if *cfgPath == "" {
		flag.Usage()
		panic("config file is required.")
	}

	if *genConfig {
		cfgDir := filepath.Dir(*cfgPath)
		info, err := os.Stat(cfgDir)
		if err != nil {
			if os.IsNotExist(err) {
				log.Error("config file directory does not exist")
				return
			}
			log.Error("error loading config file directory")
			return
		}

		if !info.IsDir() {
			log.Error("config file path invalid")
			return
		}
		var absCfgPath string
		if absCfgPath, err = filepath.Abs(*cfgPath); err != nil {
			log.Error("failed to get absolute path of config file location.")
			return
		}
		if err := WriteConfig(absCfgPath); err != nil {
			log.Error("failed to generate config file", "err", err)
			return
		}
		return
	} else {
		if info, err := os.Stat(*cfgPath); err != nil || info.IsDir() {
			log.Error("cannot open config file", "err", err)
			return
		}
	}

	cfg, err := LoadConfig(*cfgPath)
	if err != nil {
		log.Error("error loading config file", "err", err)
		return
	}
	err = CheckConfig(cfg)
	if err != nil {
		log.Error("there was a problem in the config file", "err", err)
		return
	}

	log.Info("Using configuration file", "cfg", cfg)

	raftNodeLogger := rootLogger.Named("raftNode")
	node := StartRaft(*bootstrap, *peersList, cfg, raftNodeLogger)

	vNodeDir := filepath.Join(cfg.BasePath, "vnodes")
	workerDir := filepath.Join(cfg.BasePath, "workers")
	_ = os.RemoveAll(workerDir)

	if err := os.MkdirAll(vNodeDir, 0755); err != nil {
		log.Error("Failed to create directory to store vNode data", "err", err)
		panic("Failed to create directory to store vNode data")
	}
	if err := os.MkdirAll(workerDir, 0755); err != nil {
		log.Error("Failed to create directory to store worker data", "err", err)
		panic("Failed to create directory to store worker data")
	}

	ctx := context.Background()

	log.Info("Starting vNode manager")
	vNodeLogger := rootLogger.Named("vNode")
	vnm := vnode.NewVNodeManager(vNodeDir, node, vNodeLogger)

	log.Info("Starting HTTP server", "HttpBindAddr", cfg.HttpBindAddr)
	httpLogger := rootLogger.Named("httpServer")
	s := server.New(node, httpLogger, vnm)
	go s.Run(cfg.HttpBindAddr)

	time.Sleep(500 * time.Millisecond) // small safety delay

	for _ = range cfg.Storage.NumVNodes {
		vNodeID, err := vnm.AddVNode(cfg.Storage.MaxStorage)
		if err != nil {
			log.Error("Error creating new vNode", "vNodeID", vNodeID, "err", err)
		}
		log.Info("Created new vNode", "vNodeID", vNodeID)
	}
	vnm.Run(ctx)

	dlcWorkerLogger := rootLogger.Named("dlcWorker")
	workerUID := uuid.NewString()
	workerCfg := workers.WorkerConfig{
		WorkerUID:  workerUID,
		WorkerType: "dlc",
		WorkDir:    filepath.Join(workerDir, workerUID),
	}
	dlcCfg := workers.DlcRunnerModuleConfig{
		PythonBinPath:    "/home/livelycarpet87/miniforge3/envs/DEEPLABCUT/bin/python3",
		PythonWorkerPath: "/home/livelycarpet87/Documents/GitHub/Raft-DLCWrmTrk/raftnode/workers/dlc_worker.py",
		StepTime:         0.1,
		DlcCfgPath:       "/home/livelycarpet87/Documents/GitHub/Raft-DLCWrmTrk/raftnode/testing/DLC-WrmTrk-Tyllis Xu-2025-10-25/config.yaml",
		DlcShuffle:       5,
	}
	dlcRunnerModule, _ := workers.NewDlcRunnerModule(dlcCfg, vnm, dlcWorkerLogger)
	dlcRunner, _ := workers.NewRunner(workerCfg, dlcRunnerModule, node, dlcWorkerLogger)
	go dlcRunner.Run(ctx)

	go node.ClusterMaintenance()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	<-sig
}

func CheckConfig(cfg *Config) error {
	if cfg.NodeID == "NODE_ID_CHANGE_ME" {
		return errors.New("Please set the `node_id` in the config file")
	} else if cfg.FailureDomain == "CHANGE_ME_FAILURE_DOMAIN_Room_A113" {
		return errors.New("Please set the `failure_domain` in the config file")
	}
	//TODO: Ensure base_path is valid
	//TODO: Test sufficient storage space available
	//TODO: Ensure the two addresses are valid
	return nil
}

func declareUp(peers []string, logger hclog.Logger, cfg *Config) {
	logger.Info("Searching for leadership information from peers", "peers", peers)
	leader := discoverLeader(peers)
	logger.Info("Found leader", "leader", leader)
	form := url.Values{}
	form.Add("nodeID", cfg.NodeID)
	form.Add("raftAddr", cfg.RaftBindAddr)
	form.Add("failureDomain", cfg.FailureDomain)
	form.Add("httpAddr", cfg.HttpPublicAddr)

	resp, err := http.PostForm(
		leader+"/raft/join",
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

func StartRaft(bootstrap bool, peersList string, cfg *Config, logger hclog.Logger) *raftnode.Node {
	peers := strings.Split(peersList, ",")

	fsmDir := filepath.Join(cfg.BasePath, "fsm")
	snapshotDir := filepath.Join(cfg.BasePath, "raft", "data")
	if err := os.Mkdir(fsmDir, 0755); err != nil && !os.IsExist(err) {
		logger.Error("Failed to create directory to store FSM", "err", err)
		panic("Failed to create directory to store FSM")
	}
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		logger.Error("Failed to create directory to store RAFT data", "err", err)
		panic("Failed to create directory to store RAFT data")
	}

	logger.Info("Starting RAFT", "RaftBindAddr", cfg.RaftBindAddr)
	node, err := raftnode.NewNode(
		cfg.BasePath,
		cfg.NodeID,
		cfg.RaftBindAddr,
		cfg.FailureDomain,
		cfg.HttpPublicAddr,
		logger, bootstrap)
	if err != nil {
		logger.Error("Failed to create RAFT node", "err", err)
		panic("Failed to create RAFT node")
	}

	if !bootstrap {
		readOnlyTx, _ := node.FSM.GetReadOnlyTx(context.Background())
		hRows, _ := readOnlyTx.Query(`SELECT http_addr FROM nodes`)
		for hRows.Next() {
			var httpAddr string
			hRows.Scan(&httpAddr)
			peers = append(peers, httpAddr)
		}
		hRows.Close()
		readOnlyTx.Rollback()

		go declareUp(peers, logger, cfg)
	}
	return node
}
