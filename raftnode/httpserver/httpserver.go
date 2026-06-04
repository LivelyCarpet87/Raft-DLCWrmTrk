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

	if (cmdEnv.Command == "AddNode"){
		var cmd raftcommands.AddNodeCommand
    	json.Unmarshal(cmdEnv.Data, &cmd)

		if err := s.RaftNode.AddRaftNode(cmd.NodeID, cmd.RaftAddr); err != nil {
			c.JSON(500, gin.H{
				"status":  "failed to add raft node",
			})
			return
		}

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
	router.POST("/apply", s.ApplyCommand)
	router.GET("/leader", s.WhoisLeader)
	return s
}