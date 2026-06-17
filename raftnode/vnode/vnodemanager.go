package vnode

import (
	"context"
	"encoding/json"
	"errors"
	"io"
    "path/filepath"
	"database/sql"

	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"

	rc "raft-dlcwrmtrk/raftcommands"
	"raft-dlcwrmtrk/raftnode"
	// rt "raft-dlcwrmtrk/httpserver/responsetypes"
)

type VNodeManager struct {
	VNodeDir string
	VNodes map[string]*VNode
	RaftNode *raftnode.Node
	Logger hclog.Logger
}

func NewVNodeManager(vNodeDir string, raftNode *raftnode.Node, logger hclog.Logger) *VNodeManager {
	vnm := &VNodeManager{
		VNodeDir: vNodeDir,
		VNodes: make(map[string]*VNode),
		RaftNode: raftNode,
		Logger: logger,
	}
	return vnm
}

func (vnm * VNodeManager) AddVNode(sizeLimit int64) (string,error) {
	vNodeID := uuid.NewString()
	addVNodeCommand := rc.AddVNodeCommand{
		NodeID: vnm.RaftNode.GetRaftNodeID(),
		VNodeID: vNodeID,
		SizeLimit: sizeLimit,
	}
	cmdData, _ := json.Marshal(addVNodeCommand)
	cmdEnv := rc.CommandEnvelope{
		Command: "AddVNode",
		Data: cmdData,
	}

	if err := vnm.RaftNode.ProxyApply(cmdEnv); err != nil {
		vnm.Logger.Error("Failed to add vNode", "vNodeID", vNodeID, "err", err)
		return "", err
	}
	vNode, err :=  NewVNode(vNodeID, filepath.Join(vnm.VNodeDir, vNodeID), vnm.RaftNode, vnm.Logger)
	if err != nil {
		return "", err
	}
	vnm.VNodes[vNodeID] = vNode
	return vNodeID, nil
}

func (vnm *VNodeManager) IngestFile(fileData io.ReadSeeker, ext string, ctx context.Context) (
	hash string,
	vNodeID string,
	fileSize int64,
	err error,
) {
	readOnlyTx, err := vnm.RaftNode.GetReadOnlyTx(ctx)
	defer readOnlyTx.Rollback()
	if (err != nil) {
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
		return "", "",  0, errors.New("could not find a suitable vNode")
	}

	if hash, fileSize, err = vnm.VNodes[vNodeID].IngestFile(fileData, ext); err != nil {
		vnm.Logger.Error("vNode failed to ingest file", "err", err)
        return "", "", 0, err
	}

	return hash, vNodeID, fileSize, nil
}

	}

