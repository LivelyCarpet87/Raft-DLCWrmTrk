package workers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"raft-dlcwrmtrk/raftcommands"
	"raft-dlcwrmtrk/raftnode"
	"time"

	"github.com/hashicorp/go-hclog"
)

type WorkerConfig struct {
	WorkerUID  string
	WorkerType string
	WorkDir    string
}

type BaseWorker struct {
	cfg      WorkerConfig
	raftNode *raftnode.Node
	logger   hclog.Logger
	ctx      context.Context

	jobID          string
	attemptCounter int
}

func NewBaseWorker(
	cfg WorkerConfig, raftNode *raftnode.Node, logger hclog.Logger,
) (*BaseWorker, error) {
	if err := os.RemoveAll(cfg.WorkDir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.WorkDir, 0755); err != nil {
		return nil, err
	}
	return &BaseWorker{
		cfg:            cfg,
		raftNode:       raftNode,
		logger:         logger,
		jobID:          "",
		attemptCounter: -1,
	}, nil
}

func (w *BaseWorker) EnrollWorker(ctx context.Context) error {
	w.ctx = ctx
	w.logger.Info("Enrolling worker", "WorkerUID", w.cfg.WorkerUID)
	enrollWorkerCmd := raftcommands.EnrollWorkerCommand{
		WorkerUID:  w.cfg.WorkerUID,
		NodeID:     w.raftNode.NodeID,
		WorkerType: w.cfg.WorkerType,
		EnrollTime: time.Now().UTC().Format(time.RFC3339Nano),
	}
	cmdData, _ := json.Marshal(enrollWorkerCmd)
	cmdEnv := raftcommands.CommandEnvelope{
		Command: "EnrollWorker",
		Data:    cmdData,
	}
	return w.raftNode.ProxyApply(cmdEnv)
}

func (w *BaseWorker) SendWorkerHeartbeat() error {
	return nil
}

func (w *BaseWorker) PollForJobs() (bool, string, int, []byte) {
	foundWork := len(w.jobID) != 0 && w.attemptCounter != -1
	if foundWork {
		panic("Asking for work when already assigned!")
	}
	tx, _ := w.raftNode.FSM.GetReadOnlyTx(w.ctx)
	defer tx.Rollback()
	var jobID string
	var attemptCounter int
	var jobContext []byte
	err := tx.QueryRow(`
		SELECT job_id, attempt_counter, job_context 
		FROM jobs
		WHERE worker_uid = ? AND status = 'assigned'
	`, w.cfg.WorkerUID).Scan(&jobID, &attemptCounter, &jobContext)
	if err == nil {
		w.jobID = jobID
		w.attemptCounter = attemptCounter
		return true, w.jobID, w.attemptCounter, jobContext
	} else if errors.Is(err, sql.ErrNoRows) {
		return false, w.jobID, w.attemptCounter, nil
	} else {
		w.logger.Warn("SQL error when polling for new jobs", "err", err)
		return false, w.jobID, w.attemptCounter, nil
	}
}

func (w *BaseWorker) SendEndJobCommand(status string, data []byte) error {
	if len(w.jobID) == 0 || w.attemptCounter == -1 {
		if status == "crashed" {
			return nil
		} else {
			panic("Tried to send work done with no active job")
		}
	}
	endJobCommand := raftcommands.EndJobCommand{
		JobID:          w.jobID,
		AttemptCounter: w.attemptCounter,
		EndTime:        time.Now().UTC().Format(time.RFC3339Nano),
		Status:         status,
		Data:           data,
	}
	cmdData, _ := json.Marshal(endJobCommand)
	cmdEnv := raftcommands.CommandEnvelope{
		Command: "EndJob",
		Data:    cmdData,
	}
	w.jobID = ""
	w.attemptCounter = -1
	return w.raftNode.ProxyApply(cmdEnv)
}
