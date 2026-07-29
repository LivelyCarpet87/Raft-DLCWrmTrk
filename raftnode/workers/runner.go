package workers

import (
	"context"
	"database/sql"
	"errors"
	"os/exec"
	"path/filepath"
	"raft-dlcwrmtrk/raftnode"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	_ "modernc.org/sqlite"
)

type RunnerModule interface {
	InitializeWorkingDir(path string) error
	InitializeIpcDb(tx *sql.Tx) error
	Validate() error
	CommandFactory(ctx context.Context, dbPath string, workDir string) *exec.Cmd
	PopulateWorkContext(workDir string, tx *sql.Tx, workContext []byte) error
	InterpretResults(workDir string, tx *sql.Tx) ([]byte, error)
	ResetIpcDb(tx *sql.Tx) error
	ExpectStdoutSilent(state string) bool
}

type Runner struct {
	ctx    context.Context
	logger hclog.Logger

	worker *BaseWorker
	rm     RunnerModule

	db     *sql.DB
	dbPath string

	cmd           *exec.Cmd
	cmdCtx        context.Context
	cmdCancel     context.CancelFunc
	sink          *ActivitySink
	mu            sync.Mutex
	lastHeartbeat time.Time
}

// -----------------------------
func NewRunner(cfg WorkerConfig, rmi interface{},
	raftNode *raftnode.Node, logger hclog.Logger,
) (*Runner, error) {
	rm, ok := rmi.(RunnerModule)
	if !ok {
		logger.Error("RunnerModule recieved does not meet interface requirements")
	}
	var err error
	r := &Runner{
		rm:     rm,
		logger: logger,
	}
	r.worker, err = NewBaseWorker(cfg, raftNode, logger)
	if err != nil {
		return nil, err
	}
	r.rm.InitializeWorkingDir(cfg.WorkDir)

	r.dbPath = filepath.Join(cfg.WorkDir, "ipc.db")
	r.db, err = sql.Open("sqlite", r.dbPath)
	if err != nil {
		r.logger.Error("failed to create SQLite db when creating supervisor", "err", err)
		return nil, err
	}

	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		job_id TEXT NOT NULL,
		attempt_counter INTEGER NOT NULL, -- Index starts at 1
		ack INTEGER DEFAULT 0,
		status TEXT,
		PRIMARY KEY(job_id, attempt_counter),
		CHECK(status IN (NULL,'done','failed','crashed'))
	);

	CREATE TABLE IF NOT EXISTS worker_state (
		heartbeat_time TEXT,
		phase TEXT,   -- idle | loading | computing | postprocessing
		CHECK(phase IN ('idle','loading','computing','postprocessing'))
	);

	PRAGMA journal_mode = WAL;
	`

	_, err = r.db.Exec(schema)
	if err != nil {
		r.logger.Error("failed to initialize DB when creating runner", "err", err)
		return nil, err
	}

	tx, err := r.db.BeginTx(context.Background(), nil)
	err = r.rm.InitializeIpcDb(tx)
	if err != nil {
		r.logger.Error("RunnerModule failed to initialize ipc DB", "err", err)
		return nil, err
	}
	tx.Commit()

	return r, nil
}

func (r *Runner) startCommand() {
	r.cmdCtx, r.cmdCancel = context.WithCancel(r.ctx)
	cmd := r.rm.CommandFactory(r.cmdCtx, r.dbPath, r.worker.cfg.WorkDir)

	cmd.Stdout = r.sink
	cmd.Stderr = r.sink

	err := cmd.Start()
	if err != nil {
		panic(err)
	}
	go r.silenceWatchdog(r.cmdCtx)
	go r.heartbeatWatchdog(r.cmdCtx)
	go r.runWatchdog(r.cmdCtx)

	r.cmd = cmd
}

func (r *Runner) resetDB() {
	tx, err := r.db.Begin()
	if err != nil {
		r.logger.Error("resetDB BEGIN failed", "err", err)
		return
	}

	_, err = tx.Exec(`PRAGMA busy_timeout = 1000;`)

	_, err = tx.Exec(`DELETE FROM jobs;`)
	if err != nil {
		r.logger.Error("resetDB DELETE jobs failed", "err", err)
		tx.Rollback()
		return
	}

	err = r.rm.ResetIpcDb(tx)
	if err != nil {
		r.logger.Error("resetDB Runner Module failed", "err", err)
		tx.Rollback()
		return
	}

	err = tx.Commit()
	if err != nil {
		r.logger.Error("resetDB COMMIT failed", "err", err)
	}
}

func (r *Runner) silenceWatchdog(ctx context.Context) {
	t := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-t.C:
			if r.cmd == nil || r.cmd.Process == nil {
				return
			}

			var currentPhase string
			if err := r.db.QueryRow(`
			SELECT phase FROM worker_state
			ORDER BY last_heartbeat DESC 
			LIMIT 1`).Scan(&currentPhase); err == nil {
				if r.rm.ExpectStdoutSilent(currentPhase) {
					continue
				}
			} else {
				continue
			}
			if r.sink == nil {
				continue
			}

			lastSeen := r.sink.LastSeen()
			if time.Since(lastSeen) > 60*time.Second {
				r.logger.Warn("Child process has been suspiciously silent. restarting", "lastSeen", lastSeen)
				r.restartWorker()
			}
		case <-ctx.Done():
			return
		}
	}
}

func (r *Runner) heartbeatWatchdog(ctx context.Context) {
	t := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-t.C:
			if r.cmd == nil || r.cmd.Process == nil {
				return
			}

			var lastHeartbeat string
			if err := r.db.QueryRow(`
			SELECT heartbeat_time FROM worker_state
			ORDER BY heartbeat_time DESC 
			LIMIT 1`).Scan(&lastHeartbeat); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				r.logger.Warn("Unable to read heartbeat from child process state", "err", err)
			} else {
				lhbt, err := time.Parse(time.RFC3339Nano, lastHeartbeat)
				if err != nil {
					r.logger.Warn("child process wrote nonsense heartbeat. restarting.", "err", err, "lastHeartbeat", lastHeartbeat)
					r.restartWorker()
				}
				r.lastHeartbeat = lhbt
			}
			lastHeartbeatTime := r.lastHeartbeat

			if time.Since(lastHeartbeatTime) > 15*time.Second {
				r.logger.Warn("child process heartbeat lost for more than 15 seconds. restarting.", "lastHeartbeat", lastHeartbeat, "t", t)
				r.restartWorker()
			}
		case <-ctx.Done():
			return
		}
	}
}

func (r *Runner) runWatchdog(ctx context.Context) {
	t := time.NewTicker(2 * time.Second)

	for {
		select {
		case <-t.C:
			if r.cmd == nil || r.cmd.Process == nil {
				r.logger.Warn("child process appears to have disappeared. restarting.")
				r.restartWorker()
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (r *Runner) restartWorker() {
	if !r.mu.TryLock() {
		return
	}
	defer r.mu.Unlock()
	if r.cmdCancel != nil {
		r.cmdCancel() // stops all watchdogs immediately
	}

	if r.cmd != nil && r.cmd.Process != nil {
		r.logger.Trace("restartWorker is killing old process")
		_ = r.cmd.Process.Kill()
		_, _ = r.cmd.Process.Wait()
		r.logger.Trace("restartWorker killed old process")
	}

	r.worker.SendEndJobCommand("crashed", nil)

	r.resetDB()

	time.Sleep(2500 * time.Millisecond)
	r.startCommand()
}

func (r *Runner) Run(ctx context.Context) {
	pollingTicker := time.NewTicker(500 * time.Millisecond)
	heartbeatTicker := time.NewTicker(30 * time.Second)

	r.logger.Info("initializing runner", "WorkerUID", r.worker.cfg.WorkerUID)
	r.ctx = ctx
	r.sink = NewActivitySink(ctx)

	r.worker.EnrollWorker(ctx)

	if err := r.rm.Validate(); err != nil {
		r.logger.Error("RunnerModule validation detected error", "err", err)
		panic("RunnerModule validation detected error")
	}

	r.lastHeartbeat = time.Now().UTC()
	r.startCommand()

	for {
		select {
		case <-pollingTicker.C:
			ctx, Done := context.WithTimeout(r.ctx, 500*time.Millisecond)
			// If running job
			row := r.db.QueryRowContext(ctx, `
				SELECT job_id, attempt_counter, ack, status FROM jobs
			`)
			var jobID string
			var attemptCounter int
			var ack int
			var nstatus sql.NullString
			err := row.Scan(&jobID, &attemptCounter, &ack, &nstatus)
			if err == nil {
				if ack == 2 {
					// Job completed
					status := nstatus.String
					tx, _ := r.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
					data, err := r.rm.InterpretResults(r.worker.cfg.WorkDir, tx)
					if err != nil {
						r.logger.Error("RunnerModule failed to interp results", "err", err)
						data = nil
						status = "crashed"
					}
					tx.Rollback()
					r.resetDB()
					r.worker.SendEndJobCommand(status, data)
				}
			} else if errors.Is(err, sql.ErrNoRows) { // No current job
				// Check if new job assigned
				foundJob, jobID, attemptCounter, data := r.worker.PollForJobs()
				if foundJob {
					tx, _ := r.db.BeginTx(r.ctx, nil)
					_, err = tx.Exec(`PRAGMA busy_timeout = 1000;`)
					err := r.rm.PopulateWorkContext(r.worker.cfg.WorkDir, tx, data)
					if err != nil {
						r.logger.Error("RunnerModule failed to populate work", "err", err)
						tx.Rollback()
						r.resetDB()
						r.worker.SendEndJobCommand("crashed", nil)
						break
					}
					tx.Exec(`INSERT INTO jobs(job_id, attempt_counter) VALUES(?,?)`,
						jobID, attemptCounter)
					tx.Commit()
				}
			} else {
				r.logger.Warn("unexpected SQL error", "err", err)
			}
			Done()
		case <-heartbeatTicker.C:
			// Send supervisor heartbeat
			r.worker.SendWorkerHeartbeat()
		case <-r.ctx.Done():
			return
		}
	}
}
