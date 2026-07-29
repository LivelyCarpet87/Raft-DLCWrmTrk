package server

import (
	"encoding/json"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gabriel-vasile/mimetype"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	rt "raft-dlcwrmtrk/httpserver/responsetypes"
	"raft-dlcwrmtrk/raftcommands"
)

func (s *HTTPServer) TryAddTag(c *gin.Context) {
	tagName := c.PostForm("tagName")
	tagType := c.PostForm("tagType")
	if tagName == "" {
		Fail(c, 400, "BAD_INPUT", "tag name is empty")
		return
	}
	if tagType != "primary" && tagType != "secondary" && tagType != "condition" {
		Fail(c, 400, "BAD_INPUT", "tag type is invalid")
		return
	}
	readOnlyTx, err := s.RaftNode.GetReadOnlyTx(c.Request.Context())
	defer readOnlyTx.Rollback()
	if err != nil {
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
	if count > 0 {
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
		Data:    cmdData,
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

	if tagType != "" && tagType != "primary" && tagType != "secondary" && tagType != "condition" {
		Fail(c, 400, "BAD_INPUT", "tag type is invalid")
		return
	}
	readOnlyTx, err := s.RaftNode.GetReadOnlyTx(c.Request.Context())
	defer readOnlyTx.Rollback()
	if err != nil {
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

func (s *HTTPServer) UpdateBatch(c *gin.Context) {
	batchUID := c.PostForm("batchUID")
	conditionsList := c.PostForm("conditions")
	batchName := c.PostForm("batchName")
	note := c.PostForm("note")

	if conditionsList == "" {
		Fail(c, 400, "BAD_INPUT", "conditions cannot be empty")
		return
	}
	if batchUID == "" {
		Fail(c, 400, "BAD_INPUT", "batchUID cannot be empty")
		return
	}
	var conditions []string

	if err := json.Unmarshal([]byte(conditionsList), &conditions); err != nil {
		Fail(c, 400, "BAD_INPUT", "could not parse conditions list")
		return
	}

	readOnlyTx, err := s.RaftNode.GetReadOnlyTx(c.Request.Context())
	defer readOnlyTx.Rollback()
	if err != nil {
		Fail(c, 503, "FSM_READ_ERR", "failed to get read-only tx")
		return
	}

	var count int
	if err := readOnlyTx.QueryRowContext(
		c.Request.Context(),
		"SELECT COUNT(batch_name) FROM batches WHERE batch_uid=?",
		batchUID).Scan(&count); err != nil {
		s.Logger.Error("SQLite3 Query Failed", "err", err)
		Fail(c, 503, "FSM_READ_ERR", "failed to check if batch_uid exists")
		return
	}
	if count != 1 {
		Fail(c, 404, "BATCH_NOT_FOUND", "batchUID not found")
		return
	}

	for condTag_i := range conditions {
		condTag := conditions[condTag_i]
		if err := readOnlyTx.QueryRowContext(
			c.Request.Context(),
			"SELECT COUNT(tag_name) FROM tags WHERE tag_name=? AND type='condition'",
			condTag).Scan(&count); err != nil {
			s.Logger.Error("SQLite3 Query Failed", "err", err)
			Fail(c, 503, "FSM_READ_ERR", "failed to count number of matching tags")
			return
		}
		if count != 1 {
			Fail(c, 400, "BAD_INPUT", "condition tag "+condTag+" provided does not exist")
			return
		}
	}
	updateBatchCommand := raftcommands.UpdateBatchCommand{
		BatchUID:   batchUID,
		BatchName:  batchName,
		Conditions: conditions,
		Note:       note,
	}
	cmdData, _ := json.Marshal(updateBatchCommand)
	cmdEnv := raftcommands.CommandEnvelope{
		Command: "UpdateBatch",
		Data:    cmdData,
	}
	if err := s.RaftNode.ProxyApply(cmdEnv); err != nil {
		Fail(c, 503, "RAFT_ERR", "failed to apply command")
		return
	}

	OK(c, 204, nil)
	return
}

func (s *HTTPServer) AddBatch(c *gin.Context) {
	batchUID := uuid.NewString()
	creationTime := time.Now().UTC().Format(time.RFC3339Nano)
	primaryTag := c.PostForm("primaryTag")
	secondaryTag := c.PostForm("secondaryTag")
	conditionsList := c.PostForm("conditions")
	batchName := c.PostForm("batchName")
	note := c.PostForm("note")
	fileHeader, err := c.FormFile("normFile")
	if err != nil {
		Fail(c, 400, "BAD_INPUT", "unable to load normalizer image")
		return
	}

	if conditionsList == "" {
		Fail(c, 400, "BAD_INPUT", "conditions cannot be empty")
		return
	}
	var conditions []string

	if err := json.Unmarshal([]byte(conditionsList), &conditions); err != nil {
		Fail(c, 400, "BAD_INPUT", "could not parse conditions list")
		return
	}

	readOnlyTx, err := s.RaftNode.GetReadOnlyTx(c.Request.Context())
	defer readOnlyTx.Rollback()
	if err != nil {
		Fail(c, 503, "FSM_READ_ERR", "failed to get read-only tx")
		return
	}

	var count int
	if err := readOnlyTx.QueryRowContext(
		c.Request.Context(),
		"SELECT COUNT(tag_name) FROM tags WHERE tag_name=? AND type='primary'",
		primaryTag).Scan(&count); err != nil {
		s.Logger.Error("SQLite3 Query Failed", "err", err)
		Fail(c, 503, "FSM_READ_ERR", "failed to count number of matching tags")
		return
	}
	if count != 1 {
		Fail(c, 400, "BAD_INPUT", "primary tag provided does not exist")
		return
	}
	if err := readOnlyTx.QueryRowContext(
		c.Request.Context(),
		"SELECT COUNT(tag_name) FROM tags WHERE tag_name=? AND type='secondary'",
		secondaryTag).Scan(&count); err != nil {
		s.Logger.Error("SQLite3 Query Failed", "err", err)
		Fail(c, 503, "FSM_READ_ERR", "failed to count number of matching tags")
		return
	}
	if count != 1 {
		Fail(c, 400, "BAD_INPUT", "primary tag provided does not exist")
		return
	}
	for condTag_i := range conditions {
		condTag := conditions[condTag_i]
		if err := readOnlyTx.QueryRowContext(
			c.Request.Context(),
			"SELECT COUNT(tag_name) FROM tags WHERE tag_name=? AND type='condition'",
			condTag).Scan(&count); err != nil {
			s.Logger.Error("SQLite3 Query Failed", "err", err)
			Fail(c, 503, "FSM_READ_ERR", "failed to count number of matching tags")
			return
		}
		if count != 1 {
			Fail(c, 400, "BAD_INPUT", "condition tag "+condTag+" provided does not exist")
			return
		}
	}

	fileData, err := fileHeader.Open()
	if err != nil {
		Fail(c, 400, "BAD_INPUT", "failed to open uploaded file")
		return
	}
	defer fileData.Close()
	mtype, err := mimetype.DetectReader(fileData)
	if err != nil {
		s.Logger.Error("failed to get detect MIME type", "err", err)
		Fail(c, 400, "BAD_INPUT", "failed to decode type of uploaded file")
		return
	}

	mimeType := mtype.String()
	if mimeType != "image/png" {
		Fail(c, 400, "BAD_INPUT", "expected PNG file for normalizer image")
		return
	}

	// Rewind
	if _, err = fileData.Seek(0, io.SeekStart); err != nil {
		s.Logger.Error("failed to get rewind seeker", "err", err)
		Fail(c, 400, "BAD_INPUT", "failed to open uploaded file")
		return
	}

	normMD5, vNodeID, fileSize, err := s.VNodeManager.IngestFile(fileData, mimeType, c.Request.Context())
	if err != nil {
		Fail(c, 507, "FILE_ERROR", "failed to save uploaded file")
		return
	}
	addBatchCommand := raftcommands.AddBatchCommand{
		BatchUID:     batchUID,
		CreationTime: creationTime,
		PrimaryTag:   primaryTag,
		SecondaryTag: secondaryTag,
		BatchName:    batchName,
		VNodeID:      vNodeID,
		NormMD5:      normMD5,
		NormFileSize: fileSize,
		Conditions:   conditions,
		Note:         note,
	}
	cmdData, _ := json.Marshal(addBatchCommand)
	cmdEnv := raftcommands.CommandEnvelope{
		Command: "AddBatch",
		Data:    cmdData,
	}
	if err := s.RaftNode.ProxyApply(cmdEnv); err != nil {
		Fail(c, 503, "RAFT_ERR", "failed to apply command")
		return
	}

	OK(c, 201, rt.CreateBatchResponse{BatchUID: batchUID})
	return
}

func (s *HTTPServer) ListBatches(c *gin.Context) {
	primaryTag := c.Query("primaryTag")
	secondaryTag := c.Query("secondaryTag")

	readOnlyTx, err := s.RaftNode.GetReadOnlyTx(c.Request.Context())
	defer readOnlyTx.Rollback()
	if err != nil {
		Fail(c, 503, "FSM_READ_ERR", "failed to get read-only tx")
		return
	}

	query := `
        SELECT 
			batch_uid, creation_time, batch_name,
			primary_tag, secondary_tag, norm_md5, note
        FROM batches
        WHERE 1=1
    `
	var args []any

	if primaryTag != "" {
		query += " AND primary_tag = ?"
		args = append(args, primaryTag)
	}
	if secondaryTag != "" {
		query += " AND secondary_tag = ?"
		args = append(args, secondaryTag)
	}

	s.Logger.Trace("Running query", "query", query)

	bRows, err := readOnlyTx.Query(query, args...)
	if err != nil {
		s.Logger.Error("SQLite3 Query Failed", "err", err)
		Fail(c, 503, "FSM_READ_ERR", "query to list batches failed")
		return
	}
	var batches []rt.ListBatchesEntry
	for bRows.Next() {
		var b rt.ListBatchesEntry
		if err := bRows.Scan(&b.BatchUID, &b.CreationTime,
			&b.BatchName, &b.PrimaryTag, &b.SecondaryTag, &b.NormMD5, &b.Note); err != nil {
			s.Logger.Error("SQLite3 Query Failed", "err", err)
			Fail(c, 503, "FSM_READ_ERR", "error parsing query results")
			bRows.Close()
			readOnlyTx.Rollback()
			return
		}
		batches = append(batches, b)
	}
	bRows.Close()

	for i := range batches {
		var conditions []string
		cRows, err := readOnlyTx.Query(
			`SELECT tag_name FROM conditions WHERE batch_uid = ?`,
			batches[i].BatchUID,
		)
		if err != nil {
			s.Logger.Error("SQLite3 Query Failed", "err", err)
			cRows.Close()
			readOnlyTx.Rollback()
			Fail(c, 503, "FSM_READ_ERR", "query to list conditions failed")
			return
		}
		for cRows.Next() {
			var t string
			if err := cRows.Scan(&t); err != nil {
				s.Logger.Error("SQLite3 Query Failed", "err", err)
				Fail(c, 503, "FSM_READ_ERR", "error parsing query results")
				cRows.Close()
				readOnlyTx.Rollback()
				return
			}
			conditions = append(conditions, t)
		}
		cRows.Close()
		batches[i].Conditions = conditions
		s.Logger.Trace("Found conditions for batch",
			"batchUID", batches[i].BatchUID,
			"conditions", conditions,
		)

		var videoMD5s []string
		vRows, err := readOnlyTx.Query(
			`SELECT src_video_md5 FROM src_videos WHERE batch_uid = ?`,
			batches[i].BatchUID,
		)
		if err != nil {
			s.Logger.Error("SQLite3 Query Failed", "err", err)
			Fail(c, 503, "FSM_READ_ERR", "query to list src videos failed")
			vRows.Close()
			readOnlyTx.Rollback()
			return
		}
		for vRows.Next() {
			var v string
			if err := vRows.Scan(&v); err != nil {
				s.Logger.Error("SQLite3 Query Failed", "err", err)
				Fail(c, 503, "FSM_READ_ERR", "error parsing query results")
				vRows.Close()
				readOnlyTx.Rollback()
				return
			}
			videoMD5s = append(videoMD5s, v)
		}
		vRows.Close()
		batches[i].VideoMD5s = videoMD5s
		s.Logger.Trace("Found videos for batch",
			"batchUID", batches[i].BatchUID,
			"videoMD5s", videoMD5s,
		)
	}
	readOnlyTx.Rollback()

	s.Logger.Trace("Found batches", "batches", batches)
	respData := rt.ListBatchesResponse{
		Batches: batches,
	}

	OK(c, 200, respData)
	return
}

func (s *HTTPServer) AddSrcVideo(c *gin.Context) {
	batchUID := c.PostForm("batchUID")
	uploadTime := time.Now().UTC().Format(time.RFC3339Nano)
	fileHeader, err := c.FormFile("videoFile")
	numIndvStr := c.PostForm("numIndv")
	if err != nil {
		Fail(c, 400, "BAD_INPUT", "unable to load video file")
		return
	}

	if batchUID == "" {
		Fail(c, 400, "BAD_INPUT", "batchUID cannot be empty")
		return
	}

	var numIndv int
	if numIndvStr == "" {
		Fail(c, 400, "BAD_INPUT", "numIndv cannot be empty")
		return
	} else if numIndv, err = strconv.Atoi(numIndvStr); err != nil {
		Fail(c, 400, "BAD_INPUT", "numIndv must be an integer")
		return
	} else if numIndv <= 0 {
		Fail(c, 400, "BAD_INPUT", "numIndv must be greater than zero")
		return
	}

	readOnlyTx, err := s.RaftNode.GetReadOnlyTx(c.Request.Context())
	defer readOnlyTx.Rollback()
	if err != nil {
		Fail(c, 503, "FSM_READ_ERR", "failed to get read-only tx")
		return
	}
	var count int
	if err := readOnlyTx.QueryRowContext(
		c.Request.Context(),
		"SELECT COUNT(batch_name) FROM batches WHERE batch_uid=?",
		batchUID).Scan(&count); err != nil {
		s.Logger.Error("SQLite3 Query Failed", "err", err)
		Fail(c, 503, "FSM_READ_ERR", "failed to check if batch_uid exists")
		return
	}
	if count != 1 {
		Fail(c, 404, "BATCH_NOT_FOUND", "batchUID not found")
		return
	}

	fileData, err := fileHeader.Open()
	if err != nil {
		Fail(c, 400, "BAD_INPUT", "failed to open uploaded file")
		return
	}
	defer fileData.Close()
	mtype, err := mimetype.DetectReader(fileData)
	if err != nil {
		s.Logger.Error("failed to get detect MIME type", "err", err)
		Fail(c, 400, "BAD_INPUT", "failed to decode type of uploaded file")
		return
	}

	filename := strings.TrimSpace(filepath.Base(fileHeader.Filename))

	mimeType := mtype.String()
	if mimeType != "video/mp4" {
		Fail(c, 400, "BAD_INPUT", "expected MP4 file for videos")
		return
	}

	// Rewind
	if _, err = fileData.Seek(0, io.SeekStart); err != nil {
		s.Logger.Error("failed to get rewind seeker", "err", err)
		Fail(c, 400, "BAD_INPUT", "failed to open uploaded file")
		return
	}

	vidMD5, vNodeID, fileSize, err := s.VNodeManager.IngestFile(fileData, mimeType, c.Request.Context())
	if err != nil {
		Fail(c, 507, "FILE_ERROR", "failed to save uploaded file")
		return
	}

	addSrcVideoCommand := raftcommands.AddSrcVideoCommand{
		BatchUID:      batchUID,
		VideoMD5:      vidMD5,
		VideoName:     filename,
		NumIndv:       numIndv,
		UploadTime:    uploadTime,
		VNodeID:       vNodeID,
		VideoFileSize: fileSize,
	}
	cmdData, _ := json.Marshal(addSrcVideoCommand)
	cmdEnv := raftcommands.CommandEnvelope{
		Command: "AddSrcVideo",
		Data:    cmdData,
	}
	if err := s.RaftNode.ProxyApply(cmdEnv); err != nil {
		Fail(c, 503, "RAFT_ERR", "failed to apply command")
		return
	}

	OK(c, 201, nil)
	return

}
