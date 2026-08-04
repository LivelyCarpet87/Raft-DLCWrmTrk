package workers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"raft-dlcwrmtrk/raftcommands/jobcontexts"
	"raft-dlcwrmtrk/raftcommands/jobresults"
	"raft-dlcwrmtrk/vnode"

	"github.com/hashicorp/go-hclog"
)

type DlcRunnerModuleConfig struct {
	PythonBinPath    string
	PythonWorkerPath string

	DlcCfgPath string
	DlcShuffle int
	StepTime   float64
}

type DlcRunnerModule struct {
	cfg    DlcRunnerModuleConfig
	vnm    *vnode.VNodeManager
	logger hclog.Logger
}

func NewDlcRunnerModule(cfg DlcRunnerModuleConfig,
	vNodeManager *vnode.VNodeManager, logger hclog.Logger) (*DlcRunnerModule, error) {
	return &DlcRunnerModule{
		cfg:    cfg,
		logger: logger,
		vnm:    vNodeManager,
	}, nil
}

func (rm *DlcRunnerModule) InitializeWorkingDir(path string) error {
	if err := os.MkdirAll(filepath.Join(path, "ingest"), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(path, "intermediates"), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(path, "outputs"), 0755); err != nil {
		return err
	}
	return nil
}

func (rm *DlcRunnerModule) InitializeIpcDb(tx *sql.Tx) error {
	_, err := tx.Exec(`
	CREATE TABLE IF NOT EXISTS job_context (
		input_path TEXT PRIMARY KEY,
		num_indv INTEGER NOT NULL,
		message TEXT,
		lab_vid TEXT
	);`)
	if err != nil {
		rm.logger.Error("failed to create job_context table", "err", err)
		return err
	}
	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS results (
		indiv TEXT,
		speed REAL,
		confidence REAL,
		warnTxt TEXT,
		PRIMARY KEY(indiv)
	);`)
	if err != nil {
		rm.logger.Error("failed to create results table", "err", err)
		return err
	}
	return nil
}

func (rm *DlcRunnerModule) CommandFactory(ctx context.Context, dbPath string, workDir string) *exec.Cmd {
	return exec.CommandContext(
		ctx,
		rm.cfg.PythonBinPath,
		rm.cfg.PythonWorkerPath,
		"--db", dbPath,
		"--workdir", workDir,
		"--dlc_cfg", rm.cfg.DlcCfgPath,
		"--shuffle", intToStr(rm.cfg.DlcShuffle),
		"--step_time", floatToStr(rm.cfg.StepTime),
	)
}

func (rm *DlcRunnerModule) PopulateWorkContext(workDir string, tx *sql.Tx, jobContext []byte) error {
	var jc jobcontexts.DlcJobContext
	err := json.Unmarshal(jobContext, &jc)
	if err != nil {
		return err
	}
	filename := rm.vnm.GetFilename(jc.VideoFileMD5, "video/mp4")
	filepath := filepath.Join(workDir, filename)
	err = rm.vnm.FetchFile(jc.VideoFileMD5, context.Background(), filepath)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO job_context(input_path, num_indv) VALUES(?,?)`, filepath, jc.NumIndv)
	return err
}

func (rm *DlcRunnerModule) InterpretResults(workDir string, tx *sql.Tx) ([]byte, error) {
	var ack int
	err := tx.QueryRow(`
	SELECT ack
	FROM jobs
	`).Scan(&ack)
	if err != nil {
		return nil, err
	}
	if ack != 2 {
		failedRes := jobresults.DlcJobResults{
			NumIndv:       0,
			Message:       "CRASHED: no message recovered",
			Entries:       nil,
			VideoFileInfo: nil,
		}
		data, _ := json.Marshal(failedRes)
		return data, nil
	}

	rows, err := tx.Query(`
	SELECT indiv, speed, confidence, warnTxt
	FROM results
	`)
	if err != nil {
		return nil, err
	}
	var Entries []jobresults.DlcJobResRow
	count := 0
	for rows.Next() {
		var r jobresults.DlcJobResRow
		rows.Scan(&r.Indv, &r.MeanSpeed, &r.Confidence, &r.WarnTxt)
		Entries = append(Entries, r)
		count += 1
	}
	rows.Close()
	var nMessage sql.NullString
	var message string
	var nLabVidePath sql.NullString
	tx.QueryRow(`SELECT message, lab_vid FROM job_context`).Scan(&nMessage, &nLabVidePath)
	if nMessage.Valid {
		message = nMessage.String
	} else {
		message = "CRASHED: no message recovered"
	}
	rm.logger.Info("Unpacking job context", "message", message)
	var videoFileInfoBytes []byte
	if nLabVidePath.Valid {
		labVideoPath := nLabVidePath.String
		f, err := os.Open(labVideoPath)
		if err != nil {
			return nil, err
		}
		hashMD5, vNodeID, filesize, err := rm.vnm.IngestFile(f, "video/mp4", context.Background())
		videoFileInfo := jobresults.DlcLabVideoFileInfo{
			HashMD5:  hashMD5,
			VNodeID:  vNodeID,
			Filesize: filesize,
		}
		f.Close()
		os.Remove(labVideoPath)
		videoFileInfoBytes, _ = json.Marshal(videoFileInfo)
	}
	res := &jobresults.DlcJobResults{
		NumIndv:       count,
		Message:       message,
		Entries:       Entries,
		VideoFileInfo: videoFileInfoBytes,
	}
	return json.Marshal(res)
}

func (rm *DlcRunnerModule) ResetIpcDb(tx *sql.Tx) error {
	_, err := tx.Exec(`DELETE FROM job_context;`)
	if err != nil {
		rm.logger.Error("resetDB DELETE job_context failed")
		return err
	}

	_, err = tx.Exec(`DELETE FROM results;`)
	if err != nil {
		rm.logger.Error("resetDB DELETE results failed")
		return err
	}
	return nil
}

func (rm *DlcRunnerModule) ExpectStdoutSilent(state string) bool {
	return state == "processing"
}

func (rm *DlcRunnerModule) Validate() error {
	// 1. python exists + runs
	cmd := exec.Command(rm.cfg.PythonBinPath, "--version")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("python invalid: %w (%s)", err, string(out))
	}

	// 2. worker exists
	info, err := os.Stat(rm.cfg.PythonWorkerPath)
	if err != nil {
		return fmt.Errorf("worker.py not found: %w", err)
	}

	if !info.Mode().IsRegular() {
		return fmt.Errorf("worker.py is not a regular file")
	}

	return nil
}
