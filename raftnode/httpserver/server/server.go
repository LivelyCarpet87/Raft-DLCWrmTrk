package server

import (
	"net/http"
	"io"
	"time"
	"encoding/json"

	"github.com/gin-gonic/gin"
	"github.com/hashicorp/go-hclog"

	"raft-dlcwrmtrk/raftnode"
	"raft-dlcwrmtrk/raftcommands"
	rt "raft-dlcwrmtrk/httpserver/responsetypes"
)

type HTTPServer struct {
    RaftNode *raftnode.Node
	Router *gin.Engine
	logger hclog.Logger
}

func OK(c *gin.Context, status int, data any) {
	c.JSON(status, rt.Response[any]{
		Success: true,
		Data:    &data,
	})
}

func Fail(c *gin.Context, status int, code, message string) {
	c.JSON(status, rt.Response[any]{
		Success: false,
		Error:   &rt.ErrorInfo{Code: code, Message: message},
	})
}

func (s *HTTPServer) RedirectIfNotLeader(c *gin.Context) bool {
	if (!s.RaftNode.IsLeader()) {
		leaderHttpAddr, err := s.RaftNode.GetLeaderHttpAddr()
		if (err != nil){
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
		s.logger.Error("Failed to unpack command envelope", "err", err)
		Fail(c, 400, "BAD_COMMAND_ENVELOPE", "unable to unmarshal")
		return
	}

	applyFuture :=  s.RaftNode.Raft.Apply(commandBytes, 5*time.Second)
	applyErr := applyFuture.Error()
	if (applyErr != nil){
		s.logger.Error("Error applying command", "err",applyErr)
		Fail(c, 503, "RAFT_ERROR", "error when applying command")
		return
	}

	OK(c, 200, nil)
	return
}

func (s *HTTPServer) WhoisLeader(c *gin.Context) {
	leaderHttpAddr, err := s.RaftNode.GetLeaderHttpAddr()
	if (err != nil) {
		Fail(c, 503, "NO_RAFT_LEADER", "there is no RAFT leader elected")
		return
	}
	data := rt.LeaderHttpAddrResponse{
		Leader: leaderHttpAddr,
	}
	OK(c, 200, data)
	return
}

func (s *HTTPServer) JoinCluster(c *gin.Context) {
	if s.RedirectIfNotLeader(c) {
		return
	}

	nodeID := c.PostForm("nodeID")
	failureDomain := c.PostForm("failureDomain")
    raftAddr := c.PostForm("raftAddr")
	httpAddr := c.PostForm("httpAddr")

	err := s.RaftNode.AddRaftNode(nodeID, failureDomain, raftAddr, httpAddr)
	if (err != nil) {
		Fail(c, 503, "RAFT_ERROR", "failed to add node")
		return
	}
	OK(c,200,nil)
	return
}

func (s *HTTPServer) Run(addr string) error {
    return s.Router.Run(addr)
}

func New(raftNode *raftnode.Node, logger hclog.Logger) *HTTPServer {
	router := gin.Default()
	s := &HTTPServer{
    	RaftNode: raftNode,
		Router: router,
		logger: logger,
	}

	raftApi := router.Group("/raft")
	{
		raftApi.POST("/join", s.JoinCluster)
		raftApi.POST("/apply", s.ApplyCommand)
		raftApi.GET("/leader", s.WhoisLeader)
		// raftApi.POST("/list", s.ApplyCommand)
	}

	/* 
	api := router.Group("/api")
	{
		filer := api.Group("/filer")
		{
			filer.GET("/:fileMD5", getUser)
		}
		experiment := api.Group("/experiment")
		{
			tags := experiment.Group("/tags")
			{
				tags.POST("/create", listUsers)
				tags.GET("/list", getUser)
				tags.POST("/rm", getUser)
				tags.POST("/update", getUser)
			}
			batches := experiment.Group("/batches")
			{
				batches.POST("/create", listUsers)
				batches.GET("/list", getUser)
				batches.GET("/get", getUser)
				batches.POST("/update", getUser)
			}
			videos := experiment.Group("/videos")
			{
				videos.POST("/upload", a)
				videos.GET("/status", a)
			}
		}
		metrics := api.Group("/metrics")
		{
			metrics.GET("/health", listUsers)
			metrics.GET("/status", getUser)
		}
		auth := api.Group("/auth")
		{
			auth.GET("/login", listUsers)
			auth.GET("/logout", getUser)
		}
	}
	*/
	return s
}