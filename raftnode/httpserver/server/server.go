package server

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/go-hclog"

	"raft-dlcwrmtrk/raftnode"
	rt "raft-dlcwrmtrk/httpserver/responsetypes"
	"raft-dlcwrmtrk/vnode"
)

type HTTPServer struct {
    RaftNode *raftnode.Node
	Router *gin.Engine
	Logger hclog.Logger
	VNodeManager *vnode.VNodeManager
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

func New(raftNode *raftnode.Node, Logger hclog.Logger, vnm *vnode.VNodeManager) *HTTPServer {
	router := gin.Default()
	s := &HTTPServer{
    	RaftNode: raftNode,
		Router: router,
		Logger: Logger,
		VNodeManager: vnm,
	}
	router.Use(cors.Default())

	raftApi := router.Group("/raft")
	{
		raftApi.POST("/join", s.JoinCluster)
		raftApi.POST("/apply", s.ApplyCommand)
		raftApi.GET("/leader", s.WhoisLeader)
		// raftApi.POST("/list", s.ApplyCommand)
	}

	
	api := router.Group("/api")
	{
		
		filer := api.Group("/filer")
		{
			filer.GET("/:fileMD5", s.serveFile)
		}
		
		experiment := api.Group("/experiment")
		{
			tags := experiment.Group("/tags")
			{
				tags.POST("/create", s.TryAddTag)
				tags.GET("/list", s.ListTags)
				//tags.POST("/rm", getUser)
				//tags.POST("/update", getUser)
			}
			
			batches := experiment.Group("/batches")
			{
				batches.POST("/create", s.AddBatch)
				//batches.GET("/list", getUser)
				//batches.GET("/get", getUser)
				batches.POST("/update", s.UpdateBatch)
			}
			
			videos := experiment.Group("/videos")
			{
				videos.POST("/upload", s.AddSrcVideo)
				//videos.GET("/status", a)
			}
			
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