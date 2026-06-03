package fsm

import (
	"database/sql"
	_ "modernc.org/sqlite"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"

	"raft-dlcwrmtrk/raftcommands"
)

type FSM struct {
	mu sync.Mutex
	db *sql.DB
	path string
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
		grpc_addr TEXT NOT NULL UNIQUE,
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

	CREATE TABLE IF NOT EXISTS tag_pool (
		tag_name TEXT NOT NULL,
		type TEXT NOT NULL, -- primary|secondary|condition
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
		upload_time TEXT NOT NULL,
		PRIMARY KEY(src_video_md5, batch_uid)
	);

	CREATE TABLE IF NOT EXISTS labeled_videos (
		labeled_video_md5 TEXT PRIMARY KEY,
		src_video_md5 TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS replications (
		file_md5 TEXT NOT NULL,
		vnode_id TEXT NOT NULL,
		type TEXT NOT NULL, -- original|replica|temporary
		status TEXT NOT NULL, -- pending|done|failed|timeout
		last_heartbeat_time TEXT NOT NULL,
		file_size INTEGER NOT NULL,
		PRIMARY KEY(file_md5, vnode_id),
		CHECK(type IN ('original','replica','temporary')),
		CHECK(status IN ('pending','done','failed','timeout'))
	);

	CREATE TABLE IF NOT EXISTS video_workers (
		worker_uid TEXT PRIMARY KEY,
		node_id TEXT NOT NULL,
		last_heartbeat_time TEXT NOT NULL,
		status TEXT NOT NULL, -- free|assigned|down
		CHECK(status IN ('free','assigned','down'))
	);

	CREATE TABLE IF NOT EXISTS norm_workers (
		worker_uid TEXT PRIMARY KEY,
		node_id TEXT NOT NULL,
		last_heartbeat_time TEXT NOT NULL,
		status TEXT NOT NULL, -- free|assigned|down
		CHECK(status IN ('free','assigned','down'))
	);

	CREATE TABLE IF NOT EXISTS video_jobs (
		job_uid TEXT PRIMARY KEY,
		enrollment_time TEXT NOT NULL,
		file_md5 TEXT NOT NULL,
		status TEXT NOT NULL, -- pending|assigned|done|failed|crashed|timeout
		assignment_time TEXT,
		worker_uid TEXT,
		end_time TEXT,
		CHECK(status IN ('pending','assigned','done','failed','crashed','timeout'))
	);

	CREATE TABLE IF NOT EXISTS norm_jobs (
		job_uid TEXT PRIMARY KEY,
		enrollment_time TEXT NOT NULL,
		file_md5 TEXT NOT NULL,
		status TEXT NOT NULL, -- pending|assigned|done|failed|crashed|timeout
		assignment_time TEXT,
		worker_uid TEXT,
		end_time TEXT,
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
	`)
	return err
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

	tx, err := f.db.Begin()
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
			INSERT OR REPLACE INTO nodes(node_id,raft_addr,grpc_addr,http_addr,failure_domain,status)
			VALUES(?,?,?,?,?,'up')
		`, cmd.NodeUID, cmd.RaftAddr, cmd.GrpcAddr, cmd.HttpAddr, cmd.FailureDomain)
		if err != nil {
			f.logger.Error("AddNode failed", "err", err)
			return err
		}

	default:
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