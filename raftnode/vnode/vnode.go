package vnode

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
    "os"
    "path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"

	"raft-dlcwrmtrk/raftnode"
)

type VNode struct {
	vNodeID string
	vNodePath string
	RaftNode *raftnode.Node
	Logger hclog.Logger
}

func NewVNode(vNodeID string, vNodePath string, raftNode *raftnode.Node, logger hclog.Logger) (*VNode, error) {
	if err := os.MkdirAll(filepath.Join(vNodePath,"ingest"), 0755); err != nil {
		logger.Error("Failed to create directory to store ingest files", "vNodeID", vNodeID, "err", err)
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(vNodePath,"data"), 0755); err != nil {
		logger.Error("Failed to create directory to store files", "vNodeID", vNodeID, "err", err)
		return nil, err
	}
	
	vn := &VNode{
		vNodeID: vNodeID,
		vNodePath: vNodePath,
		RaftNode: raftNode,
		Logger: logger,
	}

	return vn, nil
}

func (vn *VNode) IngestFile(fileData io.ReadSeeker, ext string) (
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

	filename := hash + ext
	ingestPath := filepath.Join(vn.vNodePath, "ingest", filename)
	if err := os.Rename(tempFileName, ingestPath); err != nil {
		if !os.IsExist(err) {
			vn.Logger.Error("failed to rename tmp file", "tempFileName", tempFileName, "ingestPath", ingestPath)
			return "", 0, err
		}
	}
	return hash, fileSize, nil
}
