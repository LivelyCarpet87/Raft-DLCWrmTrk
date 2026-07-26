package workersupervisor

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"raft-dlcwrmtrk/raftcommands"
	"raft-dlcwrmtrk/raftnode"
	"raft-dlcwrmtrk/vnode"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	_ "modernc.org/sqlite"
)

// =============================
// CONFIG STRUCT
// =============================
type SupervisorConfig struct {
	WorkerUID  string
	workerType string
	WorkDir    string
}

type CmdFactory func(ctx context.Context, dbPath string, workDir string) *exec.Cmd

// =============================
// SUPERVISOR CLASS
// =============================
type Supervisor struct {
	cfg    SupervisorConfig
	ctx    context.Context
	db     *sql.DB
	dbPath string
	Logger hclog.Logger

	RaftNode     *raftnode.Node
	VNodeManager *vnode.VNodeManager

	cmdFactory CmdFactory
	cmd        *exec.Cmd
	cmdCtx     context.Context
	cmdCancel  context.CancelFunc
	sink       *ActivitySink
	mu         sync.Mutex

	onWorkerCrash func()
}

// -----------------------------
func NewSupervisor(cfg SupervisorConfig, factory CmdFactory, RaftNode *raftnode.Node, Logger hclog.Logger) (*Supervisor, error) {
	_ = os.RemoveAll(cfg.WorkDir)
	_ = os.MkdirAll(filepath.Join(cfg.WorkDir, "ingest"), 0755)
	_ = os.MkdirAll(filepath.Join(cfg.WorkDir, "intermediates"), 0755)
	_ = os.MkdirAll(filepath.Join(cfg.WorkDir, "outputs"), 0755)

	dbPath := filepath.Join(cfg.WorkDir, "ipc.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		Logger.Error("failed to create SQLite db when creating supervisor", "err", err)
		return nil, err
	}

	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		job_id TEXT PRIMARY KEY,
		ack INTEGER DEFAULT 0,
		status TEXT,
		CHECK(status IN (NULL,'done','failed','crashed'))
	);

	CREATE TABLE IF NOT EXISTS worker_state (
		heartbeat_time TEXT,
		phase TEXT,   -- idle | loading | computing | postprocessing
		CHECK(phase IN ('idle','loading','computing','postprocessing'))
	);
	`

	_, err = db.Exec(schema)
	if err != nil {
		Logger.Error("failed to initialize DB when creating base supervisor", "err", err)
		return nil, err
	}

	return &Supervisor{
		cfg:        cfg,
		db:         db,
		dbPath:     dbPath,
		RaftNode:   RaftNode,
		Logger:     Logger,
		cmdFactory: factory,
	}, nil
}

func (s *Supervisor) startWorker() {
	s.cmdCtx, s.cmdCancel = context.WithCancel(s.ctx)
	cmd := s.cmdFactory(s.cmdCtx, s.dbPath, s.cfg.WorkDir)

	cmd.Stdout = s.sink
	cmd.Stderr = s.sink

	err := cmd.Start()
	if err != nil {
		panic(err)
	}
	go s.silenceWatchdog(s.cmdCtx)
	go s.heartbeatWatchdog(s.cmdCtx)
	go s.runWatchdog(s.cmdCtx)

	s.cmd = cmd
}

func (s *Supervisor) BeginTx(ctx context.Context) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, nil)
}

func (s *Supervisor) resetDB() {
	tx, err := s.db.Begin()
	if err != nil {
		s.Logger.Error("resetDB BEGIN failed", "err", err)
		return
	}

	_, err = tx.Exec(`DELETE FROM worker_state;`)
	if err != nil {
		s.Logger.Error("resetDB DELETE failed", "err", err)
		tx.Rollback()
		return
	}

	err = tx.Commit()
	if err != nil {
		s.Logger.Error("resetDB COMMIT failed", "err", err)
	}
}

func (s *Supervisor) silenceWatchdog(ctx context.Context) {
	t := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-t.C:
			if s.cmd == nil || s.cmd.Process == nil {
				return
			}

			var currentPhase string
			if err := s.db.QueryRow(`
			SELECT phase FROM worker_state
			ORDER BY last_heartbeat DESC 
			LIMIT 1`).Scan(&currentPhase); err == nil {
				if currentPhase != "computing" {
					continue
				}
			}
			if s.sink == nil {
				continue
			}

			lastSeen := s.sink.LastSeen()
			if time.Since(lastSeen) > 60*time.Second {
				s.Logger.Warn("DLC has been suspiciously silent. restarting", "lastSeen", lastSeen)
				s.restartWorker()
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *Supervisor) heartbeatWatchdog(ctx context.Context) {
	t := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-t.C:
			if s.cmd == nil || s.cmd.Process == nil {
				return
			}

			var lastHeartbeat string
			if err := s.db.QueryRow(`
			SELECT phase FROM worker_state
			ORDER BY last_heartbeat DESC 
			LIMIT 1`).Scan(&lastHeartbeat); err != nil {
				continue
			}
			t, err := time.Parse(time.RFC3339Nano, lastHeartbeat)
			if err != nil {
				s.Logger.Warn("python worker wrote nonsense heartbeat. restarting.", "err", err, "lastHeartbeat", lastHeartbeat)
				s.restartWorker()
			}

			if time.Since(t) > 15*time.Second {
				s.Logger.Warn("python heartbeat lost for more than 15 seconds. restarting.", "lastHeartbeat", lastHeartbeat, "t", t)
				s.restartWorker()
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *Supervisor) runWatchdog(ctx context.Context) {
	t := time.NewTicker(2 * time.Second)

	for {
		select {
		case <-t.C:
			if s.cmd == nil || s.cmd.Process == nil {
				s.Logger.Warn("python worker appears to have terminated. restarting.")
				s.restartWorker()
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// -----------------------------
func (s *Supervisor) restartWorker() {
	if !s.mu.TryLock() {
		return
	}
	defer s.mu.Unlock()
	if s.cmdCancel != nil {
		s.cmdCancel() // stops all watchdogs immediately
	}

	if s.cmd != nil && s.cmd.Process != nil {
		s.Logger.Trace("restartWorker is killing old process")
		_ = s.cmd.Process.Kill()
		_, _ = s.cmd.Process.Wait()
		s.Logger.Trace("restartWorker killed old process")
	}

	s.onWorkerCrash()
	s.resetDB()

	time.Sleep(2500 * time.Millisecond)
	s.startWorker()
}

func (s *Supervisor) Run(ctx context.Context, onWorkerCrash func()) {
	s.Logger.Info("initializing worker supervisor", "WorkerUID", s.cfg.WorkerUID)
	s.ctx = ctx
	s.onWorkerCrash = onWorkerCrash
	s.sink = NewActivitySink(ctx)

	s.Logger.Info("starting worker", "WorkerUID", s.cfg.WorkerUID)
	s.startWorker()

	s.Logger.Info("enrolling worker", "WorkerUID", s.cfg.WorkerUID)
	enrollWorkerCmd := raftcommands.EnrollWorkerCommand{
		WorkerUID:  s.cfg.WorkerUID,
		NodeID:     s.RaftNode.NodeID,
		WorkerType: s.cfg.workerType,
		EnrollTime: time.Now().UTC().Format(time.RFC3339Nano),
	}
	cmdData, _ := json.Marshal(enrollWorkerCmd)
	cmdEnv := raftcommands.CommandEnvelope{
		Command: "EnrollWorker",
		Data:    cmdData,
	}
	if err := s.RaftNode.ProxyApply(cmdEnv); err != nil {
		panic("failed to start worker")
	}
}
