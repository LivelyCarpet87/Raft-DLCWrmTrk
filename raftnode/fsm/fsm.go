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
)

type Command struct {
	Op   string        `json:"op"`
	Args []interface{} `json:"args"`
}

type Envelope struct {
	Cmd Command `json:"cmd"`
}

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
	CREATE TABLE IF NOT EXISTS kv (
		key TEXT PRIMARY KEY,
		value TEXT
	);

	CREATE TABLE IF NOT EXISTS nodes (
		id TEXT PRIMARY KEY,
		raft_addr TEXT,
		http_addr TEXT,
		state TEXT
	);

	CREATE TABLE IF NOT EXISTS meta (
		key TEXT PRIMARY KEY,
		value TEXT
	);
	`)
	return err
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.logger.Debug("applying new entry", "log", log)

	var e Envelope
	if err := json.Unmarshal(log.Data, &e); err != nil {
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

	cmd := e.Cmd

	switch cmd.Op {

	// ---- KV ----
	case "set":
		_, err := tx.Exec(
			`INSERT OR REPLACE INTO kv(key,value) VALUES(?,?)`,
			cmd.Args[0], cmd.Args[1],
		)
		if err != nil {
			return err
		}

	case "delete":
		_, err := tx.Exec(`DELETE FROM kv WHERE key=?`, cmd.Args[0])
		if err != nil {
			return err
		}

	// ---- NODE MGMT ----
	case "add_node":
		_, err := tx.Exec(`
			INSERT OR REPLACE INTO nodes(id,raft_addr,http_addr,state)
			VALUES(?,?,?,'active')
		`, cmd.Args[0], cmd.Args[1], cmd.Args[2])
		if err != nil {
			return err
		}

	case "remove_node":
		_, err := tx.Exec(`
			UPDATE nodes SET state='removed' WHERE id=?
		`, cmd.Args[0])
		if err != nil {
			return err
		}

	default:
		return errors.New("unknown op: " + cmd.Op)
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