package server

import (
	"encoding/json"

	"github.com/gin-gonic/gin"

	"raft-dlcwrmtrk/raftcommands"
	rt "raft-dlcwrmtrk/httpserver/responsetypes"
)

func (s *HTTPServer) TryAddTag(c *gin.Context) {
	tagName := c.PostForm("tagName")
	tagType := c.PostForm("tagType")
	if tagName == "" {
		Fail(c, 400, "BAD_INPUT", "tag name is empty")
		return
	}
	if (tagType != "primary" && tagType != "secondary" && tagType != "condition") {
		Fail(c, 400, "BAD_INPUT", "tag type is invalid")
		return
	}
	readOnlyTx, err := s.RaftNode.GetReadOnlyTx(c.Request.Context())
	defer readOnlyTx.Rollback()
	if (err != nil) {
		Fail(c, 503, "FSM_READ_ERR", "failed to get read-only tx")
		return
	}

	var count int
	sqlQueryErr := readOnlyTx.QueryRowContext(
		c.Request.Context(), 
		"SELECT COUNT(tag_name) FROM tags WHERE tag_name=? AND type=?", 
		tagName, tagType).Scan(&count)
	if sqlQueryErr != nil {
		s.Logger.Error("SQLite3 Query Failed", "err", sqlQueryErr)
		Fail(c, 503, "FSM_READ_ERR", "failed to count number of matching tags")
		return
	}
	readOnlyTx.Rollback()
	if (count > 0) {
		Fail(c, 400, "BAD_INPUT", "tag already exists")
		return
	}

	tryAddTagCommand := raftcommands.TryAddTagCommand{
		TagName: tagName,
		TagType: tagType,
	}
	cmdData, _ := json.Marshal(tryAddTagCommand)
	cmdEnv := raftcommands.CommandEnvelope{
		Command: "TryAddTag",
		Data: cmdData,
	}
	if err := s.RaftNode.ProxyApply(cmdEnv); err != nil {
		Fail(c, 503, "RAFT_ERR", "failed to apply command")
		return
	}

	OK(c, 200, nil)
	return
}

func (s *HTTPServer) ListTags(c *gin.Context) {
	tagType := c.Query("tagType")
	showHidden := c.DefaultQuery("showHidden", "false") == "true"


	if (tagType != "" && tagType != "primary" && tagType != "secondary" && tagType != "condition") {
		Fail(c, 400, "BAD_INPUT", "tag type is invalid")
		return
	}
	readOnlyTx, err := s.RaftNode.GetReadOnlyTx(c.Request.Context())
	defer readOnlyTx.Rollback()
	if (err != nil) {
		Fail(c, 503, "FSM_READ_ERR", "failed to get read-only tx")
		return
	}

	query := `
        SELECT tag_name, type, visible
        FROM tags
        WHERE 1=1
    `
    var args []any

    if tagType != "" {
        query += " AND type=?"
        args = append(args, tagType)
    }

    if !showHidden {
        query += " AND visible"
    }

	s.Logger.Trace("Running query", "query", query)

    rows, err := readOnlyTx.Query(query, args...)
    if err != nil {
		s.Logger.Error("SQLite3 Query Failed", "err", err)
		Fail(c, 503, "FSM_READ_ERR", "query to list tags failed")
        return
    }
    defer rows.Close()

    var tags []rt.TagInfo
    for rows.Next() {
        var t rt.TagInfo
        if err := rows.Scan(&t.TagName, &t.TagType, &t.Visible); err != nil {
			s.Logger.Error("SQLite3 Query Failed", "err", err)
			Fail(c, 503, "FSM_READ_ERR", "error parsing query results")
            return
        }
        tags = append(tags, t)
    }
	readOnlyTx.Rollback()

	s.Logger.Trace("Found tags", "tags", tags)
	respData := rt.ListTagsResponse{
		Tags: &tags,
	}

	OK(c, 200, respData)
	return
}