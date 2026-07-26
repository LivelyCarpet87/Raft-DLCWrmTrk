package server

import (
	"database/sql"

	"github.com/gin-gonic/gin"
)

func (s *HTTPServer) serveFile(c *gin.Context) {
	fileMD5 := c.Param("fileMD5")
	readOnlyTx, err := s.RaftNode.GetReadOnlyTx(c.Request.Context())
	defer readOnlyTx.Rollback()
	if err != nil {
		Fail(c, 503, "FSM_READ_ERR", "failed to get read-only tx")
		return
	}

	var vNodeID string
	var contentType string
	var contentLength int64
	err = readOnlyTx.QueryRowContext(
		c.Request.Context(),
		`SELECT v.vnode_id, f.mime_type, f.file_size FROM files f
		JOIN vnodes v
		ON f.vnode_id = v.vnode_id
		WHERE f.file_md5=? AND v.node_id=? AND f.status='done'
		LIMIT 1`,
		fileMD5, s.RaftNode.GetRaftNodeID(),
	).Scan(&vNodeID, &contentType, &contentLength)
	if err == nil {
		// serve file
		filename := s.VNodeManager.GetFilename(fileMD5, contentType)
		reader, err := s.VNodeManager.Serve(vNodeID, filename)
		if err != nil {
			s.Logger.Error("failed to get reader for file", "err", err, "filename", filename)
			Fail(c, 503, "FS_READ_ERR", "failed to load the file")
			return
		}
		extraHeaders := map[string]string{
			"Content-Disposition": `inline; filename="` + filename + `"`,
		}
		c.DataFromReader(200, contentLength, contentType, reader, extraHeaders)
		return
	}
	if err != sql.ErrNoRows {
		s.Logger.Error("error when searching for file", "err", err)
		Fail(c, 503, "FSM_READ_ERR", "failed to search for the file")
		return
	}

	// Add lazy check if pending vNode has file?

	var redirectHttpAddr string
	if err := readOnlyTx.QueryRowContext(
		c.Request.Context(),
		`SELECT n.http_addr FROM files f
		JOIN vnodes v
		ON f.vnode_id = v.vnode_id
		JOIN nodes n
		ON n.node_id = v.node_id
		WHERE f.file_md5=? AND f.status='done'
		ORDER BY RANDOM()
		LIMIT 1`,
		fileMD5,
	).Scan(&redirectHttpAddr); err != nil {
		if err == sql.ErrNoRows {
			Fail(c, 404, "FILE_NOT_FOUND", "Could not find the file requested")
			return
		}
		s.Logger.Error("error when searching for file", "err", err)
		Fail(c, 503, "FSM_READ_ERR", "failed to search for the file")
		return
	}
	c.Redirect(308, redirectHttpAddr+"/api/file/"+fileMD5)
	return

}
