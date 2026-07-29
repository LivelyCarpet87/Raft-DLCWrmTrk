package vnode

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"

	rc "raft-dlcwrmtrk/raftcommands"
	"raft-dlcwrmtrk/raftnode"
	// rt "raft-dlcwrmtrk/httpserver/responsetypes"
)

type VNodeManager struct {
	VNodeDir string
	VNodes   map[string]*VNode
	RaftNode *raftnode.Node
	Logger   hclog.Logger
}

func NewVNodeManager(vNodeDir string, raftNode *raftnode.Node, logger hclog.Logger) *VNodeManager {
	vnm := &VNodeManager{
		VNodeDir: vNodeDir,
		VNodes:   make(map[string]*VNode),
		RaftNode: raftNode,
		Logger:   logger,
	}
	return vnm
}

func (vnm *VNodeManager) AddVNode(sizeLimit int64) (string, error) {
	vNodeID := uuid.NewString()
	addVNodeCommand := rc.AddVNodeCommand{
		NodeID:    vnm.RaftNode.GetRaftNodeID(),
		VNodeID:   vNodeID,
		SizeLimit: sizeLimit,
	}
	cmdData, _ := json.Marshal(addVNodeCommand)
	cmdEnv := rc.CommandEnvelope{
		Command: "AddVNode",
		Data:    cmdData,
	}

	if err := vnm.RaftNode.ProxyApply(cmdEnv); err != nil {
		vnm.Logger.Error("Failed to add vNode", "vNodeID", vNodeID, "err", err)
		return "", err
	}
	vNode, err := NewVNode(vNodeID, filepath.Join(vnm.VNodeDir, vNodeID), vnm.RaftNode, vnm.Logger)
	if err != nil {
		return "", err
	}
	vnm.VNodes[vNodeID] = vNode
	return vNodeID, nil
}

func (vnm VNodeManager) Run(ctx context.Context) {
	for _, vn := range vnm.VNodes {
		go vn.Run(ctx)
	}
}

func (vnm *VNodeManager) IngestFile(fileData io.ReadSeeker, mimeType string, ctx context.Context) (
	hash string,
	vNodeID string,
	fileSize int64,
	err error,
) {
	readOnlyTx, err := vnm.RaftNode.GetReadOnlyTx(ctx)
	defer readOnlyTx.Rollback()
	if err != nil {
		vnm.Logger.Error("failed get readOnlyTx", "err", err)
		return "", "", 0, err
	}

	err = readOnlyTx.QueryRowContext(
		ctx,
		`SELECT
			v.vnode_id
		FROM vnodes v
		LEFT JOIN files r
			ON r.vnode_id = v.vnode_id
		AND r.status IN ('pending', 'done')
		WHERE v.node_id = ?
		AND v.status IN ('up', 'crowded')
		GROUP BY v.vnode_id, v.storage_size
		ORDER BY v.storage_size - COALESCE(SUM(r.file_size), 0)  DESC
		LIMIT 1;`,
		vnm.RaftNode.GetRaftNodeID()).Scan(
		&vNodeID,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			vnm.Logger.Error("failed to find suitable vNode. All vNodes are full")
			return "", "", 0, errors.New("all vNodes are full")
		}
		vnm.Logger.Error("failed to find suitable vNode", "err", err)
		return "", "", 0, errors.New("could not find a suitable vNode")
	}

	if hash, fileSize, err = vnm.VNodes[vNodeID].IngestFile(fileData, mimeType); err != nil {
		vnm.Logger.Error("vNode failed to ingest file", "err", err)
		return "", "", 0, err
	}

	return hash, vNodeID, fileSize, nil
}

func GetExt(mimeType string) (ext string) {
	switch mimeType {
	case "image/png":
		ext = ".png"
	case "video/mp4":
		ext = ".mp4"
	default:
		ext = ""
	}
	return ext
}

func (vnm *VNodeManager) GetFilename(fileMD5 string, mimeType string) string {
	return fileMD5 + GetExt(mimeType)
}

func (vnm *VNodeManager) Serve(vNodeID string, filename string) (io.Reader, error) {
	return vnm.VNodes[vNodeID].Serve(filename)
}

func (vnm *VNodeManager) FetchFile(fileMD5 string, ctx context.Context, filepath string) error {
	readOnlyTx, err := vnm.RaftNode.GetReadOnlyTx(ctx)
	defer readOnlyTx.Rollback()
	if err != nil {
		return err
	}

	var vNodeID string
	var contentType string
	var contentLength int64
	err = readOnlyTx.QueryRowContext(
		ctx,
		`SELECT v.vnode_id, f.mime_type, f.file_size FROM files f
		JOIN vnodes v
		ON f.vnode_id = v.vnode_id
		WHERE f.file_md5=? AND v.node_id=? AND f.status='done'
		LIMIT 1`,
		fileMD5, vnm.RaftNode.GetRaftNodeID(),
	).Scan(&vNodeID, &contentType, &contentLength)
	if err == nil {
		// provide file
		filename := vnm.GetFilename(fileMD5, contentType)
		reader, err := vnm.Serve(vNodeID, filename)
		if err != nil {
			vnm.Logger.Error("failed to get reader for file", "err", err, "filename", filename)
			return err
		}
		f, err := os.Create(filepath)
		if err != nil {
			vnm.Logger.Info("failed to create file", "filepath", filepath, "err", err)
			return err
		}
		defer f.Close()

		_, err = io.Copy(f, reader)
		if err != nil {
			vnm.Logger.Info("failed to copy to file", "filepath", filepath, "err", err)
			return err
		}
		return nil
	} else if err != sql.ErrNoRows {
		vnm.Logger.Error("error when searching for file", "err", err)
		return err
	}

	vnm.Logger.Info("File not found locally")

	var redirectHttpAddr string
	if err := readOnlyTx.QueryRowContext(
		ctx,
		`SELECT n.http_addr FROM files f
		JOIN vnodes v
		ON f.vnode_id = v.vnode_id
		JOIN nodes n
		ON n.node_id = v.node_id
		WHERE f.file_md5=? AND f.status='done'
		ORDER BY RANDOM()
		LIMIT 1`,
		fileMD5,
	).Scan(&redirectHttpAddr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			vnm.Logger.Error("file not found")
			return errors.New("VNM: file not found")
		}
		vnm.Logger.Error("error when searching for file", "err", err)
		return err
	}

	url := redirectHttpAddr + "/api/filer/" + fileMD5
	vnm.Logger.Info("Fetching file from remote", "url", url)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		vnm.Logger.Info("Remote file fetch failed", "url", url, "err", err)
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		vnm.Logger.Info("Remote file fetch failed", "url", url, "err", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		vnm.Logger.Info("Remote file fetch failed", "url", url, "status", resp.StatusCode)
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}

	f, err := os.Create(filepath)
	if err != nil {
		vnm.Logger.Info("failed to create file", "filepath", filepath, "err", err)
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, resp.Body)
	if err != nil {
		vnm.Logger.Info("failed to copy to file", "filepath", filepath, "err", err)
		return err
	}
	return nil
}
