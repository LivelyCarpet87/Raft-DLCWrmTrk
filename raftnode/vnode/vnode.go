package vnode

import (
    "os"
    "path/filepath"

	"github.com/hashicorp/go-hclog"

	"raft-dlcwrmtrk/raftnode"
)

type VNode struct {
	vNodeID string
	vNodePath string
	RaftNode *raftnode.Node
	Logger hclog.Logger
}

func NewVNode(vNodeID string, vNodePath string, raftNode *raftnode.Node, logger hclog.Logger) *VNode {
	vn := &VNode{
		vNodeID: vNodeID,
		vNodePath: vNodePath,
		RaftNode: raftNode,
		Logger: logger,
	}
	return vn
}

func (vn *VNode) IngestFile(tempFilePath string, hash string, ext string) error {
	filename := hash + ext
	ingestPath := filepath.Join(vn.vNodePath, "ingest", filename)
	if err := os.Rename(tempFilePath, ingestPath); err != nil {
		if !os.IsExist(err) {
			vn.Logger.Error("failed to rename tmp file", "tempFilePath", tempFilePath, "ingestPath", ingestPath)
			return err
		}
	}
	return nil
}