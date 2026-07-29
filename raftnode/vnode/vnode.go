package vnode

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"

	rc "raft-dlcwrmtrk/raftcommands"
	"raft-dlcwrmtrk/raftnode"
)

type VNode struct {
	vNodeID   string
	vNodePath string
	RaftNode  *raftnode.Node
	Logger    hclog.Logger
}

func NewVNode(vNodeID string, vNodePath string, raftNode *raftnode.Node, logger hclog.Logger) (*VNode, error) {
	if err := os.MkdirAll(filepath.Join(vNodePath, "ingest"), 0755); err != nil {
		logger.Error("Failed to create directory to store ingest files", "vNodeID", vNodeID, "err", err)
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(vNodePath, "data"), 0755); err != nil {
		logger.Error("Failed to create directory to store files", "vNodeID", vNodeID, "err", err)
		return nil, err
	}

	vn := &VNode{
		vNodeID:   vNodeID,
		vNodePath: vNodePath,
		RaftNode:  raftNode,
		Logger:    logger,
	}

	return vn, nil
}

func (vn *VNode) IngestFile(fileData io.ReadSeeker, mimeType string) (
	hash string,
	fileSize int64,
	err error,
) {
	h := md5.New()

	tempFile, err := os.CreateTemp("", "upload-*")
	if err != nil {
		vn.Logger.Error("failed to create tmp file", "err", err)
		return "", 0, err
	}
	defer tempFile.Close()

	tempFileName := tempFile.Name()
	defer os.Remove(tempFileName)

	mw := io.MultiWriter(tempFile, h)

	fileSize, err = io.Copy(mw, fileData)
	if err != nil {
		vn.Logger.Error("failed to copy file stream", "err", err)
		return "", 0, err
	}
	hash = hex.EncodeToString(h.Sum(nil))

	filename := hash + GetExt(mimeType)
	ingestPath := filepath.Join(vn.vNodePath, "ingest", filename)
	if err := os.Rename(tempFileName, ingestPath); err != nil {
		if !os.IsExist(err) {
			vn.Logger.Error("failed to rename tmp file", "tempFileName", tempFileName, "ingestPath", ingestPath)
			return "", 0, err
		}
	}
	return hash, fileSize, nil
}

func (vn *VNode) Serve(filename string) (io.Reader, error) {
	dataPath := filepath.Join(vn.vNodePath, "data", filename)
	_, err := os.Stat(dataPath)
	if err == nil {
		return os.Open(dataPath)
	} else if os.IsNotExist(err) {
		ingestPath := filepath.Join(vn.vNodePath, "ingest", filename)
		_, err := os.Stat(ingestPath)
		if os.IsNotExist(err) {
			vn.Logger.Error("file does not exist", "filename", filename)
			return nil, err
		} else if err != nil {
			vn.Logger.Error("failed to open ingest file for lazy-move", "ingestPath", ingestPath, "err", err)
			return nil, err
		}
		if err := os.Rename(ingestPath, dataPath); err != nil {
			if !os.IsExist(err) {
				vn.Logger.Error("failed to lazy-move ingest file", "ingestPath", ingestPath, "dataPath", dataPath)
				return nil, err
			}
		}
		return os.Open(dataPath)
	} else {
		return nil, err
	}
}

func (vn *VNode) CollectPendingFiles(ctx context.Context) {
	ingestDir := filepath.Join(vn.vNodePath, "ingest")
	dataDir := filepath.Join(vn.vNodePath, "data")

	ingestEntries, err := os.ReadDir(ingestDir)
	if err != nil {
		panic(err)
	}

	for _, entry := range ingestEntries {
		if entry.IsDir() {
			// This should not happen, but included for safety
			// especially if user opens this folder
			continue
		}
		name := entry.Name()
		dot := strings.IndexByte(name, '.')
		if dot != 32 { // expected "<32-char-md5>.<ext>"
			continue
		}
		expectedHash := strings.ToLower(name[:dot])
		ingestPath := filepath.Join(ingestDir, name)
		dataPath := filepath.Join(dataDir, name)

		f, err := os.Open(ingestPath)
		if err != nil {
			_ = os.Remove(ingestPath)
			continue
		}
		defer f.Close()

		h := md5.New()
		if _, err := io.Copy(h, f); err != nil {
			_ = os.Remove(ingestPath)
			continue
		}

		if hex.EncodeToString(h.Sum(nil)) != expectedHash {
			_ = os.Remove(ingestPath)
		}
		rotx, err := vn.RaftNode.GetReadOnlyTx(ctx)
		if err != nil {
			continue
		}
		defer rotx.Rollback()
		var fileStatus string
		if err := rotx.QueryRow(`
		SELECT file_status  FROM files
		WHERE vnode_id = ? AND file_md5 = ?
		LIMIT 1
		`, vn.vNodeID, expectedHash).Scan(&fileStatus); err != nil {
			rotx.Rollback()
			info, err := os.Stat(ingestPath)
			if err != nil {
				continue
			}

			// Ingest files are probably stale or unused after an hour
			if time.Since(info.ModTime()) > time.Hour {
				_ = os.Remove(ingestPath)
			}
			continue
		}
		rotx.Rollback()

		if err := os.Rename(ingestPath, dataPath); err != nil {
			if !os.IsExist(err) {
				vn.Logger.Error("failed to move ingest file", "ingestPath", ingestPath, "dataPath", dataPath)
				continue
			}
			_ = os.Remove(ingestPath)
			continue
		}
		statusUpdateCommand := rc.UpdateFileStatusCommand{
			FileMD5:       expectedHash,
			VNodeID:       vn.vNodeID,
			Status:        "done",
			HeartbeatTime: time.Now().UTC().Format(time.RFC3339Nano),
		}
		cmdData, _ := json.Marshal(statusUpdateCommand)
		cmdEnv := rc.CommandEnvelope{
			Command: "UpdateFileStatus",
			Data:    cmdData,
		}
		if err := vn.RaftNode.ProxyApply(cmdEnv); err != nil {
			vn.Logger.Error("failed to apply command to update file status", "err", err)
		}
	}

	// Ingest folder has been (almost) cleared
	rotx, _ := vn.RaftNode.GetReadOnlyTx(ctx)
	defer rotx.Rollback()
	rows, err := rotx.Query(
		`SELECT file_md5, mime_type FROM files
		WHERE vnode_id = ? AND status == 'pending'
		LIMIT 5`,
		vn.vNodeID,
	)
	if err != nil {
		vn.Logger.Error("failed to list pending files", "err", err)
		return
	}
	type PendingFile struct {
		fileMD5  string
		mimeType string
	}
	var pendingFiles []PendingFile
	for rows.Next() {
		var f PendingFile
		if err := rows.Scan(&f.fileMD5, &f.mimeType); err != nil {
			rows.Close()
			vn.Logger.Error("failed to list pending files", "err", err)
			return
		}
		pendingFiles = append(pendingFiles, f)
	}
	rows.Close()
	rotx.Rollback()

	for _, f := range pendingFiles {
		fileMD5 := f.fileMD5
		mimeType := f.mimeType
		var targetHttpAddr string

		rotx, _ := vn.RaftNode.GetReadOnlyTx(ctx)
		defer rotx.Rollback()
		if err := rotx.QueryRow(
			`SELECT n.http_addr 
			FROM nodes n
			LEFT JOIN vnodes v
				ON n.node_id = v.node_id
			LEFT JOIN files f
				ON v.vnode_id = f.vnode_id
			WHERE f.file_md5 = ? AND f.status == 'done'
			ORDER BY RANDOM()
			LIMIT 1`,
			fileMD5,
		).Scan(&targetHttpAddr); err != nil {
			vn.Logger.Error("failed to find provider for pending file", "err", err)
			continue
		}
		rotx.Rollback()

		getFileUrl := targetHttpAddr + "/api/filer/" + fileMD5

		resp, err := http.Get(getFileUrl)
		if err != nil {
			vn.Logger.Error("failed to request pending file over http", "getFileUrl", getFileUrl, "err", err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			vn.Logger.Error("failed to request pending file over http", "getFileUrl", getFileUrl, "resp.StatusCode", resp.StatusCode)
			continue
		}

		data, err := io.ReadAll(resp.Body)
		if err != nil {
			vn.Logger.Error("failed to read file from http req", "getFileUrl", getFileUrl, "err", err)
			statusUpdateCommand := rc.UpdateFileStatusCommand{
				FileMD5:       fileMD5,
				VNodeID:       vn.vNodeID,
				Status:        "failed",
				HeartbeatTime: time.Now().UTC().Format(time.RFC3339Nano),
			}
			cmdData, _ := json.Marshal(statusUpdateCommand)
			cmdEnv := rc.CommandEnvelope{
				Command: "UpdateFileStatus",
				Data:    cmdData,
			}
			if err := vn.RaftNode.ProxyApply(cmdEnv); err != nil {
				vn.Logger.Error("failed to apply command to update file status", "err", err)
			}
			continue
		}

		r := bytes.NewReader(data) // io.Reader, io.ReaderAt, io.Seeker

		hash, _, err := vn.IngestFile(r, mimeType)
		if err != nil || fileMD5 != hash {
			vn.Logger.Error("failed to ingest file from http req", "getFileUrl", getFileUrl, "err", err)
			statusUpdateCommand := rc.UpdateFileStatusCommand{
				FileMD5:       fileMD5,
				VNodeID:       vn.vNodeID,
				Status:        "failed",
				HeartbeatTime: time.Now().UTC().Format(time.RFC3339Nano),
			}
			cmdData, _ := json.Marshal(statusUpdateCommand)
			cmdEnv := rc.CommandEnvelope{
				Command: "UpdateFileStatus",
				Data:    cmdData,
			}
			if err := vn.RaftNode.ProxyApply(cmdEnv); err != nil {
				vn.Logger.Error("failed to apply command to update file status", "err", err)
			}
			continue
		}
		statusUpdateCommand := rc.UpdateFileStatusCommand{
			FileMD5:       fileMD5,
			VNodeID:       vn.vNodeID,
			Status:        "done",
			HeartbeatTime: time.Now().UTC().Format(time.RFC3339Nano),
		}
		cmdData, _ := json.Marshal(statusUpdateCommand)
		cmdEnv := rc.CommandEnvelope{
			Command: "UpdateFileStatus",
			Data:    cmdData,
		}
		if err := vn.RaftNode.ProxyApply(cmdEnv); err != nil {
			vn.Logger.Error("failed to apply command to update file status", "err", err)
			continue
		}
		name := hash + GetExt(mimeType)
		ingestPath := filepath.Join(ingestDir, name)
		dataPath := filepath.Join(dataDir, name)
		if err := os.Rename(ingestPath, dataPath); err != nil {
			if !os.IsExist(err) {
				vn.Logger.Error("failed to move ingest file", "ingestPath", ingestPath, "dataPath", dataPath)
			}
			continue
		}
	}
}

func (vn *VNode) Run(ctx context.Context) {
	fileAcq := time.NewTicker(1000 * time.Millisecond)
	gc := time.NewTicker(600 * time.Second)
	defer fileAcq.Stop()
	defer gc.Stop()

	for {
		select {
		case <-fileAcq.C: // Check for pending files
			vn.CollectPendingFiles(ctx)

		case <-gc.C:
			_, _ = vn.RaftNode.GetReadOnlyTx(ctx)

		case <-ctx.Done():
			return
		}
	}
}
