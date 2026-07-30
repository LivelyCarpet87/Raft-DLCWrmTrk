package server

import (
	rt "raft-dlcwrmtrk/httpserver/responsetypes"
	"strconv"

	"github.com/gin-gonic/gin"
)

func (s *HTTPServer) GetWorkersStatus(c *gin.Context) {
	workerType := c.Query("workerType")
	jobCountS := c.Query("jobCount")
	numHoursS := c.Query("numHours")
	if len(workerType) == 0 {
		Fail(c, 400, "BAD_INPUT", "need to specify workerType")
		return
	}

	var jobCount int
	var numHours int
	if len(jobCountS) > 0 {
		jobCount, err := strconv.Atoi(jobCountS)
		if err != nil || jobCount <= 0 {
			Fail(c, 400, "BAD_INPUT", "jobCount is invalid")
			return
		}
	} else {
		jobCount = -1
	}
	if len(numHoursS) > 0 {
		numHours, err := strconv.Atoi(numHoursS)
		if err != nil || numHours <= 0 {
			Fail(c, 400, "BAD_INPUT", "numHours is invalid")
			return
		}
	} else {
		numHours = -1
	}

	if jobCount < 0 && numHours < 0 {
		Fail(c, 400, "BAD_INPUT", "must specify jobCount or numHours")
		return
	}

	readOnlyTx, err := s.RaftNode.GetReadOnlyTx(c.Request.Context())
	defer readOnlyTx.Rollback()
	if err != nil {
		s.Logger.Warn("Failed to get read-only tx", "err", err)
		Fail(c, 503, "FSM_READ_ERR", "failed to get read-only tx")
		return
	}

	var args []any

	query := `
		SELECT 
			CAST(
				COALESCE(
					AVG(unixepoch(end_time) - unixepoch(assignment_time)),
					-1
				)
			AS INTEGER)
		FROM (
			SELECT assignment_time, end_time
			FROM jobs
			WHERE status IN ('done', 'crashed', 'failed')
			AND job_type = ?
	`
	args = append(args, workerType)
	if numHours > 0 {
		query += ` AND end_time >= datetime('now', '-' +?+' hours') `
		args = append(args, numHours)
	}
	query += ` ORDER BY end_time DESC `
	if jobCount > 0 {
		query += ` LIMIT ? `
		args = append(args, jobCount)
	}
	query += ` ); `

	var workerStatus rt.GetWorkersStatusResponse
	if err := readOnlyTx.QueryRow(query, args...).Scan(&workerStatus.MeanJobTime); err != nil {
		s.Logger.Warn("Unexpected SQL error", "err", err)
		Fail(c, 503, "FSM_READ_ERR", "unexpected sql error")
		return
	}

	if err := readOnlyTx.QueryRow(
		`SELECT COUNT(worker_uid)
		FROM workers
		WHERE worker_type = ? AND status IN ('free', 'assigned')`,
		workerType,
	).Scan(&workerStatus.NumWorkers); err != nil {
		s.Logger.Warn("Unexpected SQL error", "err", err)
		Fail(c, 503, "FSM_READ_ERR", "unexpected sql error")
		return
	}

	if err := readOnlyTx.QueryRow(
		`SELECT COUNT(*)
		FROM jobs
		WHERE job_type = ? AND status IN ('pending', 'assigned')`,
		workerType,
	).Scan(&workerStatus.QueueLength); err != nil {
		s.Logger.Warn("Unexpected SQL error", "err", err)
		Fail(c, 503, "FSM_READ_ERR", "unexpected sql error")
		return
	}

	OK(c, 200, workerStatus)
	return
}
