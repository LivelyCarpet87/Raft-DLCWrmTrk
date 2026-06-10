package vnode

import (
	"crypto/md5"
	"encoding/json"
	"encoding/hex"
	"io"
    "os"
    //"path/filepath"

	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/gabriel-vasile/mimetype"

	rc "raft-dlcwrmtrk/raftcommands"
	"raft-dlcwrmtrk/raftnode"
	// rt "raft-dlcwrmtrk/httpserver/responsetypes"
)

type VNodeManager struct {
	VNodes map[string]*VNode
	RaftNode *raftnode.Node
	Logger hclog.Logger
}

func NewVNodeManager(raftNode *raftnode.Node, logger hclog.Logger) *VNodeManager {
	vnm := &VNodeManager{
		VNodes: make(map[string]*VNode),
		RaftNode: raftNode,
		Logger: logger,
	}
	return vnm
}

func (vnm * VNodeManager) AddVNode(sizeLimit int) (string,error) {
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
	vnm.VNodes[vNodeID] = NewVNode(vNodeID, "./", vnm.RaftNode, vnm.Logger)
	return vNodeID, nil
}

func (vnm *VNodeManager) IngestFile(fileData io.ReadSeeker) (
	hash string, 
	mimeType string,
	vNodeID string,
	fileSize int64,
	err error,
) {
	h := md5.New()
    if err != nil && err != io.EOF {
		vnm.Logger.Error("failed to get file header", "err", err)
        return "", "", "", 0, err
    }
	mtype, err := mimetype.DetectReader(fileData)
	if err != nil {
		vnm.Logger.Error("failed to get detect MIME type", "err", err)
        return "", "", "", 0, err
    }
	ext := mtype.Extension()

	// Rewind
    if _, err = fileData.Seek(0, io.SeekStart); err != nil {
		vnm.Logger.Error("failed to get rewind seeker", "err", err)
        return "", "", "", 0, err
    }

	tempFile, err := os.CreateTemp("", "upload-*")
    if err != nil {
		vnm.Logger.Error("failed to create tmp file", "err", err)
        return "", "", "", 0, err
    }
	defer tempFile.Close()

    tempFileName := tempFile.Name()
    defer os.Remove(tempFileName)

	mw := io.MultiWriter(tempFile, h)

	fileSize, err = io.Copy(mw, fileData)
    if err != nil {
		vnm.Logger.Error("failed to copy file stream", "err", err)
        return "", "", "", 0, err
    }
	hash = hex.EncodeToString(h.Sum(nil))

	vNodeID = ""
	if err = vnm.VNodes[vNodeID].IngestFile(tempFileName, hash, ext); err != nil {
		vnm.Logger.Error("vNode failed to ingest file", "err", err)
        return "", "", "", 0, err
	}

	return hash, mimeType, vNodeID, fileSize, nil
}