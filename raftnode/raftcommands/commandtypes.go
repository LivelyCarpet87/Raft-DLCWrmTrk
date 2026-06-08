package raftcommands

import (
	"encoding/json"
)

type CommandType string

type CommandEnvelope struct {
    Command CommandType    `json:"command"`
    Data json.RawMessage `json:"data"`
}

type AddNodeCommand struct {
    NodeID string `json:"nodeID"`
    FailureDomain string `json:"failureDomain"`
    RaftAddr string `json:"raftAddr"`
    HttpAddr string `json:"httpAddr"`
}

type TryAddTagCommand struct {
    TagName string `json:"tagName"`
    TagType string `json:"tagType"`
}