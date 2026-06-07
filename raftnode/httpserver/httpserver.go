package httpserver

import (
	"net/http"
	"io"
	"time"
	"encoding/json"
	

	"github.com/gin-gonic/gin"
	"github.com/hashicorp/go-hclog"

	"raft-dlcwrmtrk/raftnode"
	"raft-dlcwrmtrk/raftcommands"
)

type HTTPServer struct {
    RaftNode *raftnode.Node
	Router *gin.Engine
	logger hclog.Logger
}

func (s *HTTPServer) ApplyCommand(c *gin.Context) {
	
    if (!s.RaftNode.IsLeader()) {
		c.Redirect(http.StatusTemporaryRedirect, string(s.RaftNode.GetLeader()))
	}

	commandBytes, err := io.ReadAll(c.Request.Body)
    if err != nil {
        c.JSON(400, gin.H{"error": "invalid body"})
        return
    }

	var cmdEnv raftcommands.CommandEnvelope
	if err := json.Unmarshal(commandBytes, &cmdEnv); err != nil {
		s.logger.Error("Failed to unpack", "err", err)
		c.JSON(400, gin.H{
			"status":  "failed to unpack command envelope",
		})
		return
	}

	applyFuture :=  s.RaftNode.Raft.Apply(commandBytes, 5*time.Second)
	applyErr := applyFuture.Error()
	if (applyErr != nil){
		s.logger.Error("Error applying command", "applyErr",applyErr)
		c.JSON(500, gin.H{
			"status":  "failed",
		})
		return
	}

	c.JSON(200, gin.H{
      "status":  "posted",
    })
}

func (s *HTTPServer) WhoisLeader(c *gin.Context) {
	c.JSON(200, gin.H{
      "leader":  string(s.RaftNode.GetLeader()),
    })
}

func (s *HTTPServer) JoinCluster(c *gin.Context) {
	if (!s.RaftNode.IsLeader()) {
		c.Redirect(http.StatusTemporaryRedirect, string(s.RaftNode.GetLeader()))
	}

	nodeID := c.PostForm("nodeID")
	failureDomain := c.PostForm("failureDomain")
    raftAddr := c.PostForm("raftAddr")
	httpAddr := c.PostForm("httpAddr")

	err := s.RaftNode.AddRaftNode(nodeID, failureDomain, raftAddr, httpAddr)
	if (err != nil) {
		c.JSON(500, gin.H{
			"status":  "failed",
		})
		return
	}
	c.JSON(200, gin.H{
			"status":  "success",
		})
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