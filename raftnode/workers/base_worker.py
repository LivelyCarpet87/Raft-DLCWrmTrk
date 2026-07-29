#!/usr/bin/env python3
import argparse
import sqlite3
import time
import os
import traceback
import threading
from datetime import datetime, timezone
import time

# -----------------------------
# SHARED STATE
# -----------------------------

class SharedState:
    def __init__(self):
        self._lock = threading.Lock()
        self._phase = "idle"

    def set_phase(self, value: str):
        assert(value in ['idle','loading','computing','postprocessing'])
        with self._lock:
            self._phase = value

    def get_phase(self) -> str:
        with self._lock:
            return self._phase


class BaseWorker():
    def __init__(self):
        self.db_path = ""
        self.work_dir = ""
        self.parser = argparse.ArgumentParser()
        self.state = SharedState()
        self.exit = False
    
    def enroll_args(self):
        self.parser.add_argument("--db", required=True)
        self.parser.add_argument("--workdir", required=True)

    def parse_args(self):
        self.args = self.parser.parse_args()
        self.db_path = self.args.db
        self.work_dir = self.args.workdir

    def init_work_dir(self):
        os.makedirs(self.work_dir, exist_ok=True)

    def connect(self):
        return sqlite3.connect(self.db_path, timeout=30)

    def get_a_job_id(self):
        con = self.connect()
        cur = con.cursor()
        job_id = cur.execute("""
                    UPDATE jobs
                    SET ack = 1
                    WHERE rowid = (
                        SELECT rowid
                        FROM jobs
                        WHERE ack = 0
                        LIMIT 1
                    )
                    RETURNING job_id;
                """).fetchone()
        con.commit()
        con.close()
        return job_id

    def mark_job_status(self, job_id, status):
        con = self.connect()
        cur = con.cursor()
        cur.execute("""
            UPDATE jobs
            SET ack = 2, status = ?
            WHERE job_id = ?;
        """,[status, job_id])
        con.commit()
        con.close()
        

    def heartbeat_loop(self):
        while not self.exit:
            phase = self.state.get_phase()
            con = self.connect()
            cur = con.cursor()
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            cur.execute("""
            INSERT INTO worker_state(heartbeat_time, phase)
            VALUES (?,?)""", (timestamp, phase))
            cur.execute("""
            DELETE FROM worker_state
            WHERE heartbeat_time NOT IN (
                SELECT heartbeat_time
                FROM worker_state
                ORDER BY heartbeat_time DESC
                LIMIT 120
            );
            """)
            con.commit()
            con.close()
            time.sleep(5)

