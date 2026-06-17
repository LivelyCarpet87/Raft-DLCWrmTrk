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

type AddVNodeCommand struct {
    NodeID string `json:"nodeID"`
    VNodeID string `json:"vNodeID"`
    SizeLimit int64 `json:"sizeLimit"`
}

type AddBatchCommand struct {
    BatchUID string `json:"batchUID"`
    CreationTime string `json:"creationTime"`
    PrimaryTag string `json:"primaryTag"`
    SecondaryTag string `json:"secondaryTag"`
    BatchName string `json:"batchName"`
    VNodeID string `json:"vNodeID"`
    NormMD5 string `json:"normMD5"`
    NormFileSize int64 `json:"normFileSize"`
    Conditions []string `json:"conditions"`
    Note string `json:"note"`
}