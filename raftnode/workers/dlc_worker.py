#!/usr/bin/env python3
from base_worker import BaseWorker

import argparse
import sqlite3
import time
import os
import traceback
import threading
from datetime import datetime, timezone
import time
import numpy as np
import math
import ffmpeg
import cv2

import deeplabcut as dlc

class VideoWorker(BaseWorker):
    def __init__(self):
        super().__init__()
        self.dlc_cfg_path = ""
        self.shuffle = -1
        self.step_time = 0
        self.skeleton = ['pharynx-tip', 'pharynx-end', '1/4-point', '3/8-point', 'midpoint', '5/8-point', '3/4-point', '7/8-point', 'tail-tip']
        self.trancode_for_browser = True

    def enroll_args(self):
        super().enroll_args()
        self.parser.add_argument("--dlc_cfg", required=True)
        self.parser.add_argument("--shuffle", type=int, required=True)
        self.parser.add_argument("--step_time", type=float, required=True)

    def parse_args(self):
        super().parse_args()
        self.dlc_cfg_path = self.args.dlc_cfg
        self.shuffle = self.args.shuffle
        self.step_time = self.args.step_time

    def run_dlc(self, video_path, numInd):
        dlc.analyze_videos(
            self.dlc_cfg_path,
            [video_path],
            videotype=".mp4",
            shuffle=self.shuffle,
            n_tracks=numInd,
            save_as_csv=True,
            destfolder=os.path.join(self.work_dir, "intermediates"),
        )

    def get_body_pos_never_null_query_generator(self):
        query = "SELECT \n"

        for label_i in range(len(self.skeleton)):
            query += f"tb{label_i}.x_pos AS lb{label_i}_x, tb{label_i}.y_pos AS lb{label_i}_y,\n"
        
        query = query[:-2] + '\n'
        query += "FROM labels AS tb0 \n"

        for label_i in range(1,len(self.skeleton)):
            query += f"JOIN labels AS tb{label_i} ON tb{label_i}.frame_num = tb0.frame_num AND tb{label_i}.indiv = tb0.indiv "
            query += f"AND tb{label_i}.bodypart = '{self.skeleton[label_i]}' AND tb{label_i}.x_pos IS NOT NULL AND tb{label_i}.y_pos IS NOT NULL \n"
        
        query += "WHERE \n"
        query += f"tb0.bodypart = '{self.skeleton[0]}' AND \n"
        query += "tb0.indiv = ? AND \n"
        query += "tb0.x_pos IS NOT NULL AND \n"
        query += "tb0.y_pos IS NOT NULL;"
        # print(query)
        return query

    def process_results(self, input_path, intended_numIndv):
        memCon = sqlite3.connect(':memory:')
        memCur = memCon.cursor()

        memCur.execute('''
            CREATE TABLE labels (
                frame_num INTEGER,
                indiv TEXT,
                bodypart TEXT,
                x_pos REAL,
                y_pos REAL,
                confidence REAL,
                UNIQUE(frame_num, indiv, bodypart, x_pos, y_pos)
            )
        ''')
        memCur.execute('''
            CREATE TABLE distance_moved (
                frame_num INTEGER,
                indiv TEXT,
                bodypart TEXT,
                distance REAL,
                UNIQUE(frame_num, indiv, bodypart, distance)
            )
        ''')
        memCur.execute('''
            CREATE TABLE individual_stats (
                indiv TEXT PRIMARY KEY,
                avg_length REAL,
                avg_speed REAL
            )
        ''')

        intermediates_dir = os.path.join(self.work_dir, "intermediates")
        filename_head = os.path.basename(input_path)[:-4]
        filename = [entry for entry in os.listdir(intermediates_dir) if entry.startswith(filename_head) and entry.endswith('.csv')][0]
        lines = []
        with open(os.path.join(self.work_dir, "intermediates",filename), mode='r', newline='', encoding='utf-8') as file:
            lines = file.readlines()[1:]

        data = {}

        """
        data : {
            indx: time
        """

        individuals_keys = lines[0].strip().split(',')[1:]
        parts_keys = lines[1].strip().split(',')[1:]
        numInd = len(set(individuals_keys))

        min_frame = -1
        max_frame = -1

        for line in lines[3:]:
            entries = line.strip().split(',')
            frame_num = int(entries[0])
            for label_i in range(0,int((len(entries)-1)/3)):
                indv = individuals_keys[label_i*3+1] # get the individual of this column
                part = parts_keys[label_i*3+1] # get the bodypart of this column
                x = entries[label_i*3+1]
                y = entries[label_i*3+2]
                if len(x) > 0 and len(y) > 0:
                    x = float(entries[label_i*3+1])
                    y = float(entries[label_i*3+2])
                else:
                    x = np.nan
                    y = np.nan
                confidence = entries[label_i*3+3]
                if len(confidence) > 0:
                    confidence = float(entries[label_i*3+3])
                else:
                    confidence = np.nan
                memCur.execute(
                    "INSERT OR REPLACE INTO labels (frame_num, indiv, bodypart, x_pos, y_pos, confidence) VALUES (?, ?, ?, ?, ?, ?)", 
                    (frame_num, indv, part, x, y,confidence)
                )
                memCon.commit()

        # Loaded data, delete intermediates
        for filename in os.listdir(intermediates_dir):
            os.remove(os.path.join(self.work_dir, "intermediates",filename))
        min_frame = memCur.execute("SELECT MIN(frame_num) FROM labels").fetchone()[0]
        max_frame = memCur.execute("SELECT MAX(frame_num) FROM labels").fetchone()[0]

        indv_lens = {}
        speed_data = {}

        for indv in [f"ind{i}" for i in range(1,numInd+1)]:
            # Calc len
            print(f"Calculating length of {indv} for {input_path}")

            memCur.execute(self.get_body_pos_never_null_query_generator(), (indv,))
            rows = memCur.fetchall()
            print(f"Got {len(rows)} perfect frames...")

            lengths = []
            for row in rows:
                length = 0.0
                for i in range(len(self.skeleton) - 1):
                    x1, y1 = row[2 * i], row[2 * i + 1]
                    x2, y2 = row[2 * (i + 1)], row[2 * (i + 1) + 1]
                    length += math.hypot( x1 - x2, y1 - y2)
                if not np.isnan(length):
                    lengths.append(length)
            if len(lengths) == 0:
                print(f"Unable to acquire median length for {indv} of {input_path}")
                continue
            indv_lens[indv] = np.median(lengths)
            print(indv_lens[indv])
            speed_data[indv] = []

        # Find FPS and Step Size
        video_path = os.path.abspath(input_path)
        src_video = cv2.VideoCapture(video_path)
        fps = src_video.get(cv2.CAP_PROP_FPS)
        step_size = int(fps*self.step_time)+1

        # Make labeled video
        out_video = cv2.VideoWriter(f'{input_path[:-4]}_labeled.mp4', 
                                    cv2.VideoWriter_fourcc(*'mp4v'), 
                                    fps / step_size, 
                                    (int(src_video.get(cv2.CAP_PROP_FRAME_WIDTH)), int(src_video.get(cv2.CAP_PROP_FRAME_HEIGHT))))
        
        if fps*self.step_time < 1:
            os.remove(input_path)
            return "failed", "ERROR: frame rate too low", None
        elif src_video.get(cv2.CAP_PROP_FRAME_COUNT) / fps < 8:
            os.remove(input_path)
            return "failed", "ERROR: video too short", None
        
        frame_width = int(src_video.get(cv2.CAP_PROP_FRAME_WIDTH))
        frame_height = int(src_video.get(cv2.CAP_PROP_FRAME_HEIGHT))
        edge_range = min(frame_width, frame_height) * 0.05

        blame:dict[str,int] = {
            "backwards movement": 0,
            "entry exit": 0,
            "circling": 0,
            "unreliable detection": 0,
        }

        for frame_ind in range(min_frame+step_size,max_frame+1, 1):
            src_video.set(cv2.CAP_PROP_POS_FRAMES, frame_ind)
            ret, frame = src_video.read()
            for indv in [f"ind{i}" for i in range(1,numInd+1)]:
                if indv not in indv_lens:
                    continue
                seg_len = indv_lens[indv] / (len(self.skeleton)-1)
                x0,y0 = memCur.execute('SELECT MIN(x_pos), MIN(y_pos) FROM labels WHERE frame_num = ? AND indiv = ?', [frame_ind, indv]).fetchone()
                x1,y1 = memCur.execute('SELECT MAX(x_pos), MAX(y_pos) FROM labels WHERE frame_num = ? AND indiv = ?', [frame_ind, indv]).fetchone()
                if x0 is None or y0 is None or x1 is None or y1 is None:
                    blame['unreliable detection'] += 1
                    continue
                elif x0 < edge_range or y0 < edge_range or x1 > frame_width-edge_range or y1 > frame_height-edge_range:
                    blame['entry exit'] += 1
                    continue
                elif (x1-x0) < 3*seg_len and (y1-y0) < 3*seg_len:
                    blame["circling"] += 1
                    continue
                cv2.rectangle(frame, (int(x0-20),int(y0-20)), (int(x1+20),int(y1+20)), (115, 158, 0), 4)
                cv2.putText(frame, indv, (int(x0-20),int(y0-25)), cv2.FONT_HERSHEY_SIMPLEX, 2, (115, 158, 0), 4, cv2.LINE_AA)



                bodypart =  self.skeleton[1]
                bodypart_pred =  self.skeleton[0]
                confidence = 0

                x_pos_prev = np.nan
                y_pos_prev = np.nan
                prev_q = memCur.execute('SELECT x_pos, y_pos, confidence FROM labels WHERE frame_num = ? AND indiv = ? AND bodypart = ?', (frame_ind-step_size, indv, bodypart) ).fetchone()
                if prev_q is not None and None not in prev_q:
                    x_pos_prev = prev_q[0]
                    y_pos_prev = prev_q[1]
                    confidence += prev_q[2] / 2

                x_pos_now = np.nan
                y_pos_now = np.nan
                now_q = memCur.execute('SELECT x_pos, y_pos, confidence FROM labels WHERE frame_num = ? AND indiv = ? AND bodypart = ?', (frame_ind, indv, bodypart) ).fetchone()
                if now_q is not None and None not in now_q:
                    x_pos_now = now_q[0]
                    y_pos_now = now_q[1]
                    confidence += now_q[2] / 2

                x_pos_pred_prev = np.nan
                y_pos_pred_prev = np.nan
                pred_q = memCur.execute('SELECT x_pos, y_pos FROM labels WHERE frame_num = ? AND indiv = ? AND bodypart = ?', (frame_ind-step_size, indv, bodypart_pred) ).fetchone()
                if pred_q is not None and None not in pred_q:
                    x_pos_pred_prev = pred_q[0]
                    y_pos_pred_prev = pred_q[1]

                if np.count_nonzero(np.isnan([x_pos_prev, y_pos_prev, x_pos_now, y_pos_now, x_pos_pred_prev, y_pos_pred_prev])):
                    distance = np.nan
                    confidence = 0

                shadow_parts_q = memCur.execute('SELECT indiv FROM labels WHERE frame_num = ? AND indiv < ? AND bodypart = ? '+
                                                        'AND x_pos between ? and ? AND y_pos between ? and ?',
                                                        (frame_ind-step_size, indv, bodypart,
                                                        x_pos_now-0.5*seg_len,x_pos_now+0.5*seg_len, y_pos_now-0.5*seg_len,y_pos_now+0.5*seg_len  )
                                                        ).fetchall()
                if len(shadow_parts_q) > 0:
                    distance = np.nan
                    confidence = 0

                pos_prev = np.array( [x_pos_prev, y_pos_prev] )
                pos_now = np.array( [x_pos_now, y_pos_now] )
                pos_pred_prev = np.array( [x_pos_pred_prev, y_pos_pred_prev] )

                distance = np.linalg.norm(pos_now-pos_prev)

                if np.dot( (pos_now-pos_prev), (pos_pred_prev-pos_now) ) < 0:
                    distance *= -1
                    if abs(distance) > seg_len*0.125:
                        blame['backwards movement'] += 1
                        confidence = 0

                if abs(distance) > seg_len*1.5:
                    distance = np.nan
                    blame['unreliable detection'] += 1
                    confidence = 0

                speed_data[indv].append([distance, confidence])
                if confidence > 0:
                    if bodypart == self.skeleton[1]:
                        cv2.circle(frame, (int(x_pos_now),int(y_pos_now)), 16, (0, 94, 213), -1)
                    else:
                        cv2.circle(frame, (int(x_pos_now),int(y_pos_now)), 16, (115, 158, 0), -1)
                    cv2.line(frame, (int(x_pos_now),int(y_pos_now)), (int(x_pos_prev),int(y_pos_prev)), (115, 158, 0), 4)
                elif None not in [x_pos_now, y_pos_now]:
                    cv2.circle(frame, (int(x_pos_now),int(y_pos_now)), 16, (0,255,255), -1)
                    if not np.count_nonzero(np.isnan([x_pos_prev, y_pos_prev])):
                        cv2.line(frame, (int(x_pos_now),int(y_pos_now)), (int(x_pos_prev),int(y_pos_prev)), (0,255,255), 4)
            out_video.write(frame)
        src_video.release()
        out_video.release()
        os.remove(input_path)
        
        if self.trancode_for_browser:
            (
                ffmpeg
                .input(f'{input_path[:-4]}_labeled.mp4')
                .output(f'{input_path[:-4]}_labeled_h264.mp4', vcodec='libx264', crf=23, preset='fast', pix_fmt='yuv420p')
                .run(overwrite_output=True)
            )
            os.replace(f'{input_path[:-4]}_labeled_h264.mp4', f'{input_path[:-4]}_labeled.mp4')

        speed_res = []
        for indv in [f"ind{i}" for i in range(1,numInd+1)]:
            if indv not in speed_data:
                continue
            print(f"Calculating speed of {indv} for {input_path}")
            weighted_avg_distance = 0
            sum_weights = 0
            for d, p in speed_data[indv]:
                if not np.isnan(d) and p > 0:
                    weighted_avg_distance += d*p
                    sum_weights += p
            print(f"Calculating speed of {indv} for {input_path}")
            if sum_weights == 0:
                raise ValueError
            speed = (weighted_avg_distance/sum_weights) /step_size*fps
            if np.isnan(speed):
                print(f"The speed of {indv} for {input_path} was NaN.")
                raise ValueError

            print("Assigning confidence value.")
            warnTxt="INFO: No warnings."
            confidence_vals = np.array(speed_data[indv])[:,1]
            confidence = sum_weights/ len(speed_data[indv])
            if confidence < 0.6:
                warnTxt = "WARNING: low model confidence in tracking."
            if np.count_nonzero(confidence_vals) < (max_frame-min_frame-step_size) * 0.45:
                continue
            elif np.count_nonzero(confidence_vals) < (max_frame-min_frame-step_size) * 0.7:
                warnTxt='WARNING: was not able to track for more than 30% of video.'
            
            speed_res.append( (indv,speed,confidence, warnTxt) )
        
        warning = ""
        if len(speed_res) == 0:
            return "failed", f"ERROR: No speed data found", None
        elif not all(row[2] for row in speed_res):
            blame["unreliable detection"] //= 2 # Underweight unreliable detections, because they are usually caused by other factors
            blame_target = max(blame, key=blame.get) # type: ignore
            warning = blame_target
        elif len(speed_res) == intended_numIndv:
            pass
        elif len(speed_data) in range(intended_numIndv-1,intended_numIndv+2):
            warning = f"incorrect count of indiv, expected {intended_numIndv}, got {len(speed_data)}"
        else:
            blame["unreliable detection"] //= 2 # Underweight unreliable detections, because they are usually caused by other factors
            blame_target = max(blame, key=blame.get) # type: ignore
            return "failed", f"ERROR: {blame_target}", None

        con = self.connect()
        cur = con.cursor()

        for indiv, speed, conf, warnTxt in speed_res:
            cur.execute("""
                INSERT OR REPLACE INTO results(indiv, speed, confidence, warnTxt)
                VALUES (?, ?, ?, ?)
            """, (indiv, float(speed), conf, warnTxt))

        con.commit()
        con.close()
        if len(warning) == 0:
            return "done", "INFO: completed without warnings", f'{input_path[:-4]}_labeled.mp4'
        return "done", f"WARNING: {warning}", f'{input_path[:-4]}_labeled.mp4'

    def service_loop(self):
        while not self.exit:
            print("polling for work")
            job = self.get_a_job_id()
            if job is None:
                self.state.set_phase('idle')
                time.sleep(5)
                continue

            job_id = job[0]
            try:
                self.state.set_phase('loading')
                con = self.connect()
                cur = con.cursor()
                job_context = cur.execute("""
                SELECT input_path, num_indv
                FROM job_context
                """).fetchone()
                con.close()
                if job_context is None:
                    print("panic")
                    self.exit = True
                    return
                input_path, numInd = job_context
                print(f"[JOB] {job_id}")

                self.state.set_phase('computing')
                # Run inference
                self.run_dlc(input_path, numInd)

                self.state.set_phase('postprocessing')
                # Process outputs into results table
                status, res_message, lab_vid = self.process_results(input_path, numInd)
                con = self.connect()
                cur = con.cursor()
                cur.execute("""
                    UPDATE job_context
                    SET 
                        message = ?,
                        lab_vid = ?;
                """,[res_message, lab_vid])
                con.commit()
                con.close()
                self.mark_job_status(job_id, status)

            except Exception as e:
                print("ERROR:", e)
                traceback.print_exc()
                con = self.connect()
                cur = con.cursor()
                error_msg = f"ERROR: {e}"
                cur.execute("""
                    UPDATE job_context
                    SET message = ?;
                """,[error_msg])
                con.commit()
                con.close()
                self.mark_job_status(job_id, "crashed")
                time.sleep(2)

    def start(self):
        print("python worker started")
        heartbeat_thread = threading.Thread(
            target=self.heartbeat_loop,
            name="heartbeat"
        )

        service_thread = threading.Thread(
            target=self.service_loop,
            name="service"
        )

        heartbeat_thread.start()
        service_thread.start()


if __name__ == "__main__":
    vid_worker = VideoWorker()
    vid_worker.enroll_args()
    vid_worker.parse_args()
    vid_worker.start()