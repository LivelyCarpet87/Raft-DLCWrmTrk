package server

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	rt "raft-dlcwrmtrk/httpserver/responsetypes"
	"raft-dlcwrmtrk/raftcommands"
)

func (s *HTTPServer) RedirectIfNotLeader(c *gin.Context) bool {
	if !s.RaftNode.IsLeader() {
		leaderHttpAddr, err := s.RaftNode.GetLeaderHttpAddr()
		if err != nil {
			Fail(c, 503, "RAFT_ERROR", "unable to find leader")
			return true
		}
		c.Redirect(http.StatusTemporaryRedirect, leaderHttpAddr)
		return true
	}
	return false
}

func (s *HTTPServer) ApplyCommand(c *gin.Context) {
	if s.RedirectIfNotLeader(c) {
		return
	}

	commandBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(400, gin.H{"error": "invalid body"})
		return
	}

	var cmdEnv raftcommands.CommandEnvelope
	if err := json.Unmarshal(commandBytes, &cmdEnv); err != nil {
		s.Logger.Error("Failed to unpack command envelope", "err", err)
		Fail(c, 400, "BAD_COMMAND_ENVELOPE", "unable to unmarshal")
		return
	}

	applyFuture := s.RaftNode.Raft.Apply(commandBytes, 5*time.Second)
	applyErr := applyFuture.Error()
	if applyErr != nil {
		s.Logger.Error("Error applying command", "err", applyErr)
		Fail(c, 503, "RAFT_ERROR", "error when applying command")
		return
	}

	OK(c, 200, nil)
	return
}

func (s *HTTPServer) WhoisLeader(c *gin.Context) {
	leaderHttpAddr, err := s.RaftNode.GetLeaderHttpAddr()
	if err != nil {
		Fail(c, 503, "NO_RAFT_LEADER", "there is no RAFT leader elected")
		return
	}
	data := rt.LeaderHttpAddrResponse{
		Leader: leaderHttpAddr,
	}
	OK(c, 200, data)
	return
}

func (s *HTTPServer) GetEndpoints(c *gin.Context) {
	readOnlyTx, err := s.RaftNode.GetReadOnlyTx(c.Request.Context())
	defer readOnlyTx.Rollback()
	if err != nil {
		s.Logger.Warn("Failed to get read-only tx", "err", err)
		Fail(c, 503, "FSM_READ_ERR", "failed to get read-only tx")
		return
	}
	hRows, err := readOnlyTx.Query("SELECT http_addr FROM nodes WHERE status = 'up'")
	if err != nil {
		s.Logger.Error("SQLite3 Query Failed", "err", err)
		Fail(c, 503, "FSM_READ_ERR", "query to list batches failed")
		return
	}
	var ret rt.GetHttpEndpointsResponse
	for hRows.Next() {
		var httpAddr string
		if err := hRows.Scan(&httpAddr); err != nil {
			s.Logger.Error("SQLite3 Query Failed", "err", err)
			Fail(c, 503, "FSM_READ_ERR", "error parsing query results")
			hRows.Close()
			readOnlyTx.Rollback()
			return
		}
		ret.Endpoints = append(ret.Endpoints, httpAddr)
	}
	hRows.Close()
	readOnlyTx.Rollback()
	OK(c, 200, ret)
	return
}

func (s *HTTPServer) JoinCluster(c *gin.Context) {
	if s.RedirectIfNotLeader(c) {
		return
	}

	nodeID := c.PostForm("nodeID") // TODO: Check that nodeID is not duplicated already in cluster
	failureDomain := c.PostForm("failureDomain")
	raftAddr := c.PostForm("raftAddr")
	httpAddr := c.PostForm("httpAddr")

	err := s.RaftNode.AddRaftNode(nodeID, failureDomain, raftAddr, httpAddr)
	if err != nil {
		Fail(c, 503, "RAFT_ERROR", "failed to add node")
		return
	}
	OK(c, 200, nil)
	return
}
