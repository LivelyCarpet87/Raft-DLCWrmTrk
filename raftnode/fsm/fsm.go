package fsm

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	_ "modernc.org/sqlite"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"

	"raft-dlcwrmtrk/raftcommands"
	"raft-dlcwrmtrk/raftcommands/jobcontexts"
)

type FSM struct {
	mu     sync.Mutex
	db     *sql.DB
	path   string
	logger hclog.Logger
}

func New(db *sql.DB, path string, logger hclog.Logger) *FSM {
	return &FSM{db: db, path: path, logger: logger}
}

type snapshot struct {
	path string
}

func InitSchema(db *sql.DB) error {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS nodes (
		node_id TEXT PRIMARY KEY,
		raft_addr TEXT NOT NULL UNIQUE,
		http_addr TEXT NOT NULL UNIQUE,
		failure_domain TEXT NOT NULL,
		status TEXT NOT NULL, -- up|down
		CHECK(status IN ('up','down'))
	);

	CREATE TABLE IF NOT EXISTS vnodes (
		vnode_id TEXT PRIMARY KEY,
		node_id TEXT NOT NULL,
		status TEXT NOT NULL, -- up|crowded|full|down
		storage_size INTEGER NOT NULL,
		CHECK(status IN ('up','crowded','full','down'))
	);

	CREATE TABLE IF NOT EXISTS tags (
		tag_name TEXT NOT NULL,
		type TEXT NOT NULL, -- primary|secondary|condition
		visible INTEGER NOT NULL DEFAULT TRUE,
		PRIMARY KEY(tag_name, type),
		CHECK(type IN ('primary','secondary','condition'))
	);

	CREATE TABLE IF NOT EXISTS batches (
		batch_uid TEXT PRIMARY KEY,
		creation_time TEXT NOT NULL,
		primary_tag TEXT NOT NULL,
		secondary_tag TEXT NOT NULL,
		batch_name TEXT NOT NULL,
		norm_md5 TEXT NOT NULL,
		note TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS conditions (
		batch_uid TEXT NOT NULL,
		tag_name TEXT NOT NULL,
		PRIMARY KEY(batch_uid, tag_name)
	);

	CREATE TABLE IF NOT EXISTS norms (
		norm_md5 TEXT PRIMARY KEY,
		creation_time TEXT NOT NULL,
		norm_labeled_md5 TEXT
	);

	CREATE TABLE IF NOT EXISTS src_videos (
		src_video_md5 TEXT NOT NULL,
		batch_uid TEXT NOT NULL,
		video_name TEXT NOT NULL,
		num_indv INTEGER NOT NULL,
		upload_time TEXT NOT NULL,
		PRIMARY KEY(src_video_md5, batch_uid)
	);

	CREATE TABLE IF NOT EXISTS labeled_videos (
		labeled_video_md5 TEXT PRIMARY KEY,
		src_video_md5 TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS files (
		file_md5 TEXT NOT NULL,
    	mime_type TEXT NOT NULL,
		vnode_id TEXT NOT NULL,
		type TEXT NOT NULL, -- original|replica|temporary
		status TEXT NOT NULL, -- pending|done|failed|timeout
		last_heartbeat_time TEXT NOT NULL,
		file_size INTEGER NOT NULL,
    	PRIMARY KEY(file_md5, vnode_id),
		CHECK(type IN ('original','replica','temporary')),
		CHECK(status IN ('pending','done','failed','timeout'))
	);

	CREATE TABLE IF NOT EXISTS workers (
		worker_uid TEXT PRIMARY KEY,
		node_id TEXT NOT NULL,
		worker_type TEXT NOT NULL,
		last_heartbeat_time TEXT NOT NULL,
		status TEXT NOT NULL, -- free|assigned|down
		CHECK(status IN ('free','assigned','down'))
	);

	CREATE TABLE IF NOT EXISTS jobs (
		job_id TEXT NOT NULL,
		attempt_counter INTEGER NOT NULL, -- Index starts at 1
		job_type TEXT NOT NULL,
		enrollment_time TEXT NOT NULL,
		status TEXT NOT NULL, -- pending|assigned|done|failed|crashed|timeout
		assignment_time TEXT,
		worker_uid TEXT,
		end_time TEXT,
		job_context BLOB,
		PRIMARY KEY(job_id, attempt_counter),
		CHECK(status IN ('pending','assigned','done','failed','crashed','timeout'))
	);

	CREATE TABLE IF NOT EXISTS tracklets (
		src_video_md5 TEXT NOT NULL,
		track_id TEXT NOT NULL,
		min_speed REAL NOT NULL,
		max_speed REAL NOT NULL,
		median_speed REAL NOT NULL,
		mean_speed REAL NOT NULL,
		track_len REAL NOT NULL, -- tracking time in seconds
		worm_len REAL NOT NULL, -- median worm length
		confidence REAL NOT NULL, -- model confidence score
		warn_txt TEXT NOT NULL, -- "GOOD" | warning text
		PRIMARY KEY(src_video_md5, track_id)
	);

	CREATE TABLE IF NOT EXISTS params (
		param_name TEXT PRIMARY KEY,
		param_value BLOB NOT NULL
	);

	INSERT OR IGNORE INTO params(param_name,param_value)
	VALUES 
		('workerHeartbeatTimeout',30), -- send heartbeat every 10 seconds
		('vNodeTransferTimeout',60), -- trigger by node, then runs every 30 seconds
		('videoJobTimeout',300), -- constant polling
		('normJobTimeout',120), -- constant polling
		('fileReplicaCount', 3), -- number of replicas to make
		('normDistance', 5.0), -- distance between normalizer markers, in millimeters
		('version', "1.0"); -- release version (for future migration detection)

	PRAGMA journal_mode=WAL;
	`)
	return err
}

func (f *FSM) GetReadOnlyTx(ctx context.Context) (*sql.Tx, error) {
	return f.db.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: true,
	})
}

type vNodeChoice struct {
	vNodeID       string
	failureDomain string
	status        string
}

func VNodeConstraintStrict(chosen []vNodeChoice, newChoice vNodeChoice) bool {
	if !VNodeConstraintAllowCrowded(chosen, newChoice) {
		return false
	} else if newChoice.status == "crowded" {
		return false
	}
	return true
}

func VNodeConstraintAllowCrowded(chosen []vNodeChoice, newChoice vNodeChoice) bool {
	failureDomainSet := make(map[string]struct{})

	for _, c := range chosen {
		failureDomainSet[c.failureDomain] = struct{}{}
	}
	if !VNodeConstraintRelaxFailureDomain(chosen, newChoice) {
		return false
	} else if _, exists := failureDomainSet[newChoice.failureDomain]; exists {
		return false
	}
	return true
}

func VNodeConstraintRelaxFailureDomain(chosen []vNodeChoice, newChoice vNodeChoice) bool {
	vNodeIDSet := make(map[string]struct{})

	for _, c := range chosen {
		vNodeIDSet[c.vNodeID] = struct{}{}
	}
	if _, exists := vNodeIDSet[newChoice.vNodeID]; exists {
		return false
	}
	return true
}

func SpreadFile(seedVNodeID string, fileMD5 string, mimeType string, filesize int64,
	creationTime string, tx *sql.Tx, logger hclog.Logger) error {
	if _, err := tx.Exec(`
		INSERT OR IGNORE INTO files(file_md5, mime_type, vnode_id, type, status, 
		last_heartbeat_time,file_size)
		VALUES(?,?,?,'original','done',?,?)
		`, fileMD5, mimeType, seedVNodeID, creationTime, filesize); err != nil {
		logger.Error("failed to record seed file upload", "err", err)
		return err
	}
	var seedFailureDomain string
	var seedStatus string
	if err := tx.QueryRow(`
	SELECT n.failure_domain, v.status FROM vnodes v
	JOIN nodes n
		ON n.node_id = v.node_id
	WHERE v.vnode_id = ?
	`, seedVNodeID).Scan(&seedFailureDomain, &seedStatus); err != nil {
		logger.Error("failed to find failure domain of vNode", "err", err)
		return err
	}

	var fileReplicaCount int
	if err := tx.QueryRow(
		"SELECT param_value FROM params WHERE param_name='fileReplicaCount'",
	).Scan(&fileReplicaCount); err != nil {
		logger.Error("failed to read param fileReplicaCount", "err", err)
		return err
	}

	var existingCopies int
	if err := tx.QueryRow(
		"SELECT COUNT(file_md5) FROM files WHERE file_md5=? AND status IN ('done', 'pending')",
		fileMD5).Scan(&existingCopies); err != nil {
		logger.Error("failed to count existing copies of file",
			"err", err, "fileMD5", fileMD5)
		return err
	}
	if existingCopies >= fileReplicaCount+1 {
		logger.Info("sufficient existing copies of file, not replicating",
			"fileMD5", fileMD5, "existingCopies", existingCopies)
		return nil
	}

	ringQuery := `
	SELECT
		v.vnode_id,
		n.failure_domain,
		v.status
	FROM vnodes v
	JOIN nodes n
		ON n.node_id = v.node_id
	WHERE v.status != 'down'
	AND v.status NOT IN ('full', 'down')
	ORDER BY
		CASE
			WHEN v.vnode_id > ? THEN 0
			ELSE 1
		END,
		v.vnode_id;`
	var selectedVNodes []vNodeChoice

	selectedVNodes = append(selectedVNodes, vNodeChoice{
		vNodeID:       seedVNodeID,
		failureDomain: seedFailureDomain,
		status:        seedStatus,
	})
	constraints := []func([]vNodeChoice, vNodeChoice) bool{
		VNodeConstraintStrict,
		VNodeConstraintAllowCrowded,
		VNodeConstraintRelaxFailureDomain,
	}
	for cn, constraint := range constraints {
		if cn == 1 {
			logger.Warn("allowing crowded during walk along vNode ring")
		} else if cn == 2 {
			logger.Warn("allowing failure domain repeats during walk along vNode ring")
		}
		rows, err := tx.Query(ringQuery, seedVNodeID)
		if err != nil {
			logger.Error("failed to walk vNode ring", "err", err)
			return err
		}
		defer rows.Close()
		for rows.Next() {
			if len(selectedVNodes) == fileReplicaCount+1 {
				break
			}

			var choice vNodeChoice
			if err := rows.Scan(
				&choice.vNodeID,
				&choice.failureDomain,
				&choice.status,
			); err != nil {
				logger.Error("ring walk SQLite3 query row read failed",
					"err", err)
				return err
			}
			if constraint(selectedVNodes, choice) {
				selectedVNodes = append(selectedVNodes, choice)
			}
		}
		if len(selectedVNodes) == fileReplicaCount+1 {
			break
		}
		rows.Close()
	}
	if len(selectedVNodes) != fileReplicaCount+1 {
		logger.Error("failed to select sufficient sites on ring",
			"fileReplicaCount", fileReplicaCount)
		return errors.New("failed to select sufficient sites on ring")
	}

	// Skip first element because seed is already done
	for _, c := range selectedVNodes[1:] {
		if _, err := tx.Exec(`
			INSERT OR IGNORE INTO files(file_md5, mime_type, vnode_id, type, status, 
			last_heartbeat_time,file_size)
			VALUES(?,?,?,'replica','pending',?,?)
			`, fileMD5, mimeType, c.vNodeID, creationTime, filesize); err != nil {
			logger.Error("failed to record seed file upload", "err", err)
			return err
		}
		var maxStorage int64
		var usedStorage int64
		if err := tx.QueryRow(`
			SELECT
				v.storage_size,
				COALESCE(SUM(r.file_size), 0) AS storage_remaining
			FROM vnodes v
			LEFT JOIN files r
				ON r.vnode_id = v.vnode_id
			AND r.status IN ('pending', 'done')
			WHERE v.vnode_id = ?
			LIMIT 1;`, c.vNodeID).Scan(&maxStorage, &usedStorage); err != nil {
			logger.Error("failed to get vNode usage stats", "err", err)
			return err
		}
		if usedStorage >= maxStorage {
			if _, err := tx.Exec(`
				UPDATE vnodes
				SET status = 'full'
				WHERE vnode_id = ?
				`, c.vNodeID); err != nil {
				logger.Error("failed to record new vNode state as full", "err", err)
				return err
			}
		} else if usedStorage >= maxStorage/10*9 {
			if _, err := tx.Exec(`
				UPDATE vnodes
				SET status = 'crowded'
				WHERE vnode_id = ?
				`, c.vNodeID); err != nil {
				logger.Error("failed to record new vNode state as crowded", "err", err)
				return err
			}
		}

	}

	return nil
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.logger.Debug("applying new entry", "log", log)

	var cmdEnv raftcommands.CommandEnvelope
	if err := json.Unmarshal(log.Data, &cmdEnv); err != nil {
		f.logger.Error("command unmarshalling failed failed", "err", err)
		return err
	}

	ctx := context.Background()
	tx, err := f.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		f.logger.Error("begin tx failed", "err", err)
		return err
	}

	committed := false
	defer func() {
		if !committed {
			f.logger.Error("did not commit. Rolling back changes.")
			_ = tx.Rollback()
		}
	}()

	switch cmdEnv.Command {
	// ---- NODE MGMT ----
	case "AddNode":
		var cmd raftcommands.AddNodeCommand
		json.Unmarshal(cmdEnv.Data, &cmd)
		_, err := tx.Exec(`
			INSERT OR REPLACE INTO nodes(node_id,raft_addr,http_addr,failure_domain,status)
			VALUES(?,?,?,?,'up')
		`, cmd.NodeID, cmd.RaftAddr, cmd.HttpAddr, cmd.FailureDomain)
		if err != nil {
			f.logger.Error("AddNode failed", "err", err)
			return err
		}

	case "TryAddTag":
		var cmd raftcommands.TryAddTagCommand
		json.Unmarshal(cmdEnv.Data, &cmd)
		_, err := tx.Exec(`
			INSERT OR IGNORE INTO tags(tag_name, type)
			VALUES(?,?)
		`, cmd.TagName, cmd.TagType)
		if err != nil {
			f.logger.Error("TryAddTag failed", "err", err)
			return err
		}

	case "AddVNode":
		var cmd raftcommands.AddVNodeCommand
		json.Unmarshal(cmdEnv.Data, &cmd)
		_, err := tx.Exec(`
			INSERT INTO vnodes(vnode_id, node_id, status, storage_size)
			VALUES(?,?,'up',?)
		`, cmd.VNodeID, cmd.NodeID, cmd.SizeLimit)
		if err != nil {
			f.logger.Error("AddVNode failed", "err", err)
			return err
		}

	case "AddBatch":
		var cmd raftcommands.AddBatchCommand
		json.Unmarshal(cmdEnv.Data, &cmd)
		if _, err := tx.Exec(`
			INSERT INTO batches(batch_uid, batch_name, creation_time, 
			primary_tag, secondary_tag, norm_md5, note)
			VALUES(?,?,?,?,?,?,?)
			`, cmd.BatchUID, cmd.BatchName, cmd.CreationTime,
			cmd.PrimaryTag, cmd.SecondaryTag,
			cmd.NormMD5, cmd.Note); err != nil {
			f.logger.Error("AddBatch failed to create batch", "err", err)
			return err
		}
		for _, cond := range cmd.Conditions {
			if _, err := tx.Exec(`
				INSERT INTO conditions(batch_uid, tag_name)
				VALUES(?,?)
				`, cmd.BatchUID, cond); err != nil {
				f.logger.Error("AddBatch failed to record condition", "err", err, "cond", cond)
				return err
			}
		}

		if err := SpreadFile(cmd.VNodeID, cmd.NormMD5, "image/png", cmd.NormFileSize,
			cmd.CreationTime, tx, f.logger); err != nil {
			f.logger.Error("AddBatch failed replicate files", "err", err)
			return err
		}

		var normDistance float64
		if err := tx.QueryRow(
			"SELECT param_value FROM params WHERE param_name='normDistance'",
		).Scan(&normDistance); err != nil {
			f.logger.Error("failed to read param normDistance", "err", err)
			return err
		}

		jobContext := jobcontexts.NormJobContext{
			NormFileMD5:  cmd.NormMD5,
			NormDistance: normDistance,
		}
		jobContextBlob, _ := json.Marshal(jobContext)

		if _, err := tx.Exec(`
			INSERT OR IGNORE INTO jobs(job_id, attempt_counter, 
			job_type, enrollment_time, 
			status, job_context)
			VALUES(?,1,'norm',?,'pending'?)
			`, cmd.NormMD5, cmd.CreationTime, jobContextBlob); err != nil {
			f.logger.Error("AddBatch failed to enroll norm job", "err", err)
			return err
		}

	case "UpdateFileStatus":
		var cmd raftcommands.UpdateFileStatusCommand
		json.Unmarshal(cmdEnv.Data, &cmd)
		if _, err := tx.Exec(`
			UPDATE files
			SET 
				status = ?, 
				last_heartbeat_time = ?
			WHERE file_md5 = ? AND vnode_id = ?
			`, cmd.Status, cmd.HeartbeatTime,
			cmd.FileMD5, cmd.VNodeID); err != nil {
			f.logger.Error("FileStatusUpdate failed to update entry", "err", err)
			return err
		}

	case "UpdateBatch":
		var cmd raftcommands.UpdateBatchCommand
		json.Unmarshal(cmdEnv.Data, &cmd)
		if _, err := tx.Exec(`
			UPDATE batches
			SET 
				batch_name = ?, 
				note = ?
			WHERE batch_uid = ?
			`, cmd.BatchName, cmd.Note, cmd.BatchUID,
		); err != nil {
			f.logger.Error("UpdateBatch failed to update entry", "err", err)
			return err
		}
		if _, err := tx.Exec(`
			DELETE FROM conditions
			WHERE batch_uid = ?
			`, cmd.BatchUID,
		); err != nil {
			f.logger.Error("UpdateBatch failed to delete existing conditions", "err", err)
			return err
		}
		for _, cond := range cmd.Conditions {
			if _, err := tx.Exec(`
				INSERT INTO conditions(batch_uid, tag_name)
				VALUES(?,?)
				`, cmd.BatchUID, cond); err != nil {
				f.logger.Error("UpdateBatch failed to record condition", "err", err, "cond", cond)
				return err
			}
		}
	case "AddSrcVideo":
		var cmd raftcommands.AddSrcVideoCommand
		json.Unmarshal(cmdEnv.Data, &cmd)
		if _, err := tx.Exec(`
			INSERT OR IGNORE INTO src_videos(src_video_md5, batch_uid, 
			video_name, num_indv, upload_time)
			VALUES(?,?,?,?,?,?)
			`, cmd.VideoMD5, cmd.BatchUID,
			cmd.VideoName, cmd.NumIndv, cmd.UploadTime); err != nil {
			f.logger.Error("AddSrcVideo failed to create video entry", "err", err)
			return err
		}
		if err := SpreadFile(cmd.VNodeID, cmd.VideoMD5, "video/mp4", cmd.VideoFileSize,
			cmd.UploadTime, tx, f.logger); err != nil {
			f.logger.Error("AddSrcVideo failed replicate files", "err", err)
			return err
		}

		jobContext := jobcontexts.DlcJobContext{
			VideoFileMD5: cmd.VideoMD5,
			NumIndv:      cmd.NumIndv,
		}
		jobContextBlob, _ := json.Marshal(jobContext)

		if _, err := tx.Exec(`
			INSERT INTO jobs(job_id, attempt_counter, 
			job_type, enrollment_time, 
			status, job_context)
			VALUES(?,1,'dlc',?,'pending'?)
			ON CONFLICT(job_id, attempt_counter)
			DO UPDATE SET
				enrollment_time = excluded.enrollment_time
				job_context = excluded.job_context
			WHERE jobs.enrollment_time < excluded.enrollment_time AND status = 'pending'
			`, cmd.VideoMD5, cmd.UploadTime, jobContextBlob); err != nil {
			f.logger.Error("AddSrcVideo failed to enroll dlc job", "err", err)
			return err
		}

	case "EnrollWorker":
		var cmd raftcommands.EnrollWorkerCommand
		json.Unmarshal(cmdEnv.Data, &cmd)
		if _, err := tx.Exec(`
			INSERT INTO workers(worker_uid, node_id, worker_type, 
			last_heartbeat_time, status)
			VALUES(?,?,?,?,"free")
			ON CONFLICT(worker_uid)
			DO UPDATE SET
				node_id = excluded.node_id,
				worker_type = excluded.worker_type,
				last_heartbeat_time = excluded.last_heartbeat_time,
				status = excluded.status
			WHERE workers.status = "down"
			`, cmd.WorkerUID, cmd.NodeID, cmd.WorkerType,
			cmd.EnrollTime); err != nil {
			f.logger.Error("Failed to enroll worker",
				"worker_uid", cmd.WorkerUID,
				"worker_type", cmd.WorkerType,
				"err", err)
			return err
		}
	default:
		f.logger.Error("unknown raft command", "cmd", cmdEnv.Command)
		return errors.New("unknown raft command: " + string(cmdEnv.Command))
	}

	if err := tx.Commit(); err != nil {
		f.logger.Error("commit failed", "err", err)
		return err
	}

	committed = true
	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	tmp := f.path + ".snapshot"

	// Ensure WAL state is flushed into main DB file
	if _, err := f.db.Exec(`PRAGMA wal_checkpoint(FULL);`); err != nil {
		return nil, err
	}

	// Use SQLite-native atomic backup operation
	_, err := f.db.Exec(`VACUUM INTO ?`, tmp)
	if err != nil {
		return nil, err
	}

	return &snapshot{path: tmp}, nil
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {

	file, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := io.Copy(sink, file); err != nil {
		sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s *snapshot) Release() {
	os.Remove(s.path)
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	tmp := filepath.Join(os.TempDir(), "raft_restore.db")

	file, err := os.Create(tmp)
	if err != nil {
		return err
	}

	if _, err := io.Copy(file, rc); err != nil {
		file.Close()
		return err
	}

	file.Sync()
	file.Close()

	rc.Close()

	// atomic replace
	if err := os.Rename(tmp, f.path); err != nil {
		return err
	}

	return nil
}

func (f *FSM) QueryHttpAddrFromRaftAddr(raftAddr string) (string, error) {
	var httpAddr string
	err := f.db.QueryRow(
		`SELECT http_addr FROM nodes WHERE raft_addr = ?`,
		raftAddr,
	).Scan(&httpAddr)
	return httpAddr, err
}
