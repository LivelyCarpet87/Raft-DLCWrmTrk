package workersupervisor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"raft-dlcwrmtrk/raftnode"
	"time"

	"github.com/hashicorp/go-hclog"
)

type VideoSupervisorConfig struct {
	SupervisorCfg SupervisorConfig
	PollMS        int

	PythonBinPath    string
	PythonWorkerPath string

	DlcCfgPath string
	DlcShuffle int
	StepTime   float64
}
type VideoSupervisor struct {
	Supervisor *Supervisor
	Logger     hclog.Logger
}

func NewVideoSupervisor(cfg VideoSupervisorConfig, RaftNode *raftnode.Node, Logger hclog.Logger) (*VideoSupervisor, error) {
	factory := func(ctx context.Context, dbPath string, workDir string) *exec.Cmd {
		return exec.CommandContext(
			ctx,
			cfg.PythonBinPath,
			cfg.PythonWorkerPath,
			"--db", dbPath,
			"--workdir", workDir,
			"--dlc_cfg", cfg.DlcCfgPath,
			"--shuffle", intToStr(cfg.DlcShuffle),
			"--step_time", floatToStr(cfg.StepTime),
		)
	}
	cfg.SupervisorCfg.workerType = "dlc"
	supervisor, err := NewSupervisor(cfg.SupervisorCfg, factory, RaftNode, Logger)
	if err != nil {
		return nil, err
	}
	schemaUpdate := `
	CREATE TABLE IF NOT EXISTS job_context (
		job_id TEXT PRIMARY KEY,
		input_path TEXT NOT NULL,
		numInd INTEGER NOT NULL,
		message TEXT
	);

	CREATE TABLE IF NOT EXISTS results (
		job_id TEXT,
		indiv TEXT,
		speed REAL,
		confidence REAL,
		PRIMARY KEY(job_id, indiv)
	);
	`
	_, err = supervisor.db.Exec(schemaUpdate)
	if err != nil {
		Logger.Error("failed to initialize DB when creating base supervisor", "err", err)
		return nil, err
	}

	return &VideoSupervisor{
		Supervisor: supervisor,
	}, nil
}

func (s *VideoSupervisor) resetDB() {
	tx, err := s.Supervisor.db.Begin()
	if err != nil {
		s.Logger.Error("resetDB BEGIN failed", "err", err)
		return
	}

	_, err = tx.Exec(`DELETE FROM job_context;`)
	if err != nil {
		s.Logger.Error("resetDB DELETE failed", "err", err)
		tx.Rollback()
		return
	}

	_, err = tx.Exec(`DELETE FROM results;`)
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

func (s *VideoSupervisor) onWorkerCrash() {

}

// -----------------------------
func (s *VideoSupervisor) readResults() {
	rows, err := s.Supervisor.db.Query(`
	SELECT job_id, indiv, speed, confidence
	FROM results
	`)
	if err != nil {
		return
	}
	defer rows.Close()

	seen := map[string]bool{}

	for rows.Next() {
		var job, indiv string
		var speed float64
		var conf int

		_ = rows.Scan(&job, &indiv, &speed, &conf)

		if !seen[job] {
			s.Logger.Info("job completed", "job", job)
			seen[job] = true
		}

		s.Logger.Info("indiv", indiv, "speed", speed, "conf", conf)
	}

	for job := range seen {
		_, _ = s.Supervisor.db.Exec(`DELETE FROM results WHERE job_id=?`, job)
		_, _ = s.Supervisor.db.Exec(`DELETE FROM job_context WHERE job_id=?`, job)
	}
}

// -----------------------------
func (s *VideoSupervisor) controlLoop(ctx context.Context) {
	pollingTicker := time.NewTicker(500 * time.Millisecond)
	heartbeatTicker := time.NewTicker(30 * time.Second)

	for {
		select {
		case <-pollingTicker.C:
			// If running job
			row := s.Supervisor.db.QueryRowContext(ctx, `
				SELECT job_id, ack, state FROM jobs
			`)
			var jobID string
			var ack int
			var state *string
			err := row.Scan(&jobID, &ack, state)
			if err == nil {
				if ack == 2 {
					// Job completed
					if *state == "done" {

					} else if *state == "failed" {

					} else if *state == "crashed" {

					}
					s.resetDB()
				}
			} else if errors.Is(err, sql.ErrNoRows) {
				// Check if job assigned
				// Start job if assigned
			} else {
				panic("unexpected SQL error")
			}

		case <-heartbeatTicker.C:
			// Check worker heartbeat
			// If running job
			// send job heartbeat
			// TODO: else send job request?
			// Send supervisor heartbeat

		case <-ctx.Done():
			return
		}
	}
}

func (s *VideoSupervisor) Run(ctx context.Context) {
	s.Supervisor.Run(ctx, s.onWorkerCrash)
}

func validatePythonSetup(pythonPath string, workerPath string) error {
	// 1. python exists + runs
	cmd := exec.Command(pythonPath, "--version")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("python invalid: %w (%s)", err, string(out))
	}

	// 2. worker exists
	info, err := os.Stat(workerPath)
	if err != nil {
		return fmt.Errorf("worker.py not found: %w", err)
	}

	if !info.Mode().IsRegular() {
		return fmt.Errorf("worker.py is not a regular file")
	}

	return nil
}
