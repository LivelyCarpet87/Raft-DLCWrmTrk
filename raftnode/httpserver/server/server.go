package server

import (
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/go-hclog"

	"raft-dlcwrmtrk/raftnode"
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

	
	api := router.Group("/api")
	{
		/*
		filer := api.Group("/filer")
		{
			filer.GET("/:fileMD5", getUser)
		}
		*/
		experiment := api.Group("/experiment")
		{
			tags := experiment.Group("/tags")
			{
				tags.POST("/create", s.TryAddTag)
				//tags.GET("/list", getUser)
				//tags.POST("/rm", getUser)
				//tags.POST("/update", getUser)
			}
			/*
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
			*/
		}
		/*
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
		*/
	}
	return s
}