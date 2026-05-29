#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import time
import queue
import threading
from datetime import datetime

import cv2
import rclpy
from rclpy.node import Node
from rclpy.qos import (
    QoSProfile,
    ReliabilityPolicy,
    HistoryPolicy,
    DurabilityPolicy,
)

from sensor_msgs.msg import Image
from mavros_msgs.msg import State
from cv_bridge import CvBridge


class SeanoCaDebugVideoLogger(Node):
    """
    SEANO CA Debug Video Logger 30 FPS

    Design:
    - Source video dari ROS Image topic, default /ca/debug_image.
    - Tidak membuka /dev/video0.
    - Tidak membuat folder mission sebelum topic image benar-benar terbaca.
    - Output MP4 dibuat constant 30 FPS.
    - Kalau source /ca/debug_image belum update, frame terakhir diulang.
    - Kalau topic mati/stale terlalu lama, writer tidak menulis freeze panjang.
    - CSV dibuat mudah dibaca untuk analisis pengujian.
    - Bisa mode test tanpa MAVROS arm.
    - Bisa mode mission asli dengan MAVROS armed/disarmed.
    """

    def __init__(self):
        super().__init__("video_logger_node")

        # ============================================================
        # SOURCE IMAGE CONFIG
        # ============================================================
        self.declare_parameter("image_topic", "/ca/debug_image")
        self.declare_parameter("image_reliability", "reliable")
        self.declare_parameter("qos_depth", 10)

        # ============================================================
        # VIDEO CONFIG: 30 FPS OUTPUT
        # ============================================================
        self.declare_parameter("output_fps", 30.0)
        self.declare_parameter("codec", "mp4v")
        self.declare_parameter("segment_seconds", 300.0)

        self.declare_parameter("constant_output_fps", True)
        self.declare_parameter("duplicate_last_frame_to_match_fps", True)

        # Untuk output 30 FPS konstan, parameter ini harus False.
        self.declare_parameter("write_only_new_frames", False)
        self.declare_parameter("enforce_max_output_fps", True)

        # Batas umur frame repeat.
        # Jika /ca/debug_image berhenti lebih dari nilai ini, logger tidak menulis freeze panjang.
        self.declare_parameter("max_repeat_frame_age_s", 1.0)
        self.declare_parameter("max_stale_frame_age_s", 2.0)

        self.declare_parameter("frame_queue_size", 10)
        self.declare_parameter("resize_changed_frame", True)

        # ============================================================
        # IMAGE BEFORE FOLDER GATE
        # ============================================================
        self.declare_parameter("require_image_before_folder", True)
        self.declare_parameter("required_image_max_age_s", 2.0)

        # ============================================================
        # STORAGE CONFIG
        # ============================================================
        self.declare_parameter("external_mount_point", "/mnt/seano/SEANO_SSD")
        self.declare_parameter(
            "local_mount_point",
            os.path.expanduser("~/Documents/SEANO_logs"),
        )

        self.declare_parameter("enable_external_logging", True)
        self.declare_parameter("enable_local_logging", True)
        self.declare_parameter("require_external_on_mission", False)
        self.declare_parameter("single_target_mode", False)

        self.declare_parameter("create_own_mission_folder_if_missing", False)
        self.declare_parameter("mission_folder_wait_sec", 0.8)
        self.declare_parameter("mission_folder_attach_timeout_sec", 5.0)

        # ============================================================
        # MISSION GATE CONFIG
        # ============================================================
        self.declare_parameter("mission_gate_topic", "/mavros/state")
        self.declare_parameter("force_record_without_mavros", False)
        self.declare_parameter("stop_on_disarm", True)

        # ============================================================
        # DIAGNOSTIC CONFIG
        # ============================================================
        self.declare_parameter("index_flush_every_n_frames", 120)
        self.declare_parameter("status_period_s", 5.0)

        # ============================================================
        # LOAD PARAMETERS
        # ============================================================
        self.image_topic = str(self.get_parameter("image_topic").value)
        self.image_reliability = str(
            self.get_parameter("image_reliability").value
        ).lower().strip()
        self.qos_depth = max(1, int(self.get_parameter("qos_depth").value))

        self.output_fps = float(self.get_parameter("output_fps").value)
        if self.output_fps <= 0.0:
            self.output_fps = 30.0

        self.codec = str(self.get_parameter("codec").value)
        if len(self.codec) != 4:
            self.codec = "mp4v"

        self.segment_seconds = float(self.get_parameter("segment_seconds").value)

        self.constant_output_fps = bool(
            self.get_parameter("constant_output_fps").value
        )
        self.duplicate_last_frame_to_match_fps = bool(
            self.get_parameter("duplicate_last_frame_to_match_fps").value
        )
        self.write_only_new_frames = bool(
            self.get_parameter("write_only_new_frames").value
        )
        self.enforce_max_output_fps = bool(
            self.get_parameter("enforce_max_output_fps").value
        )

        self.max_repeat_frame_age_s = float(
            self.get_parameter("max_repeat_frame_age_s").value
        )
        self.max_stale_frame_age_s = float(
            self.get_parameter("max_stale_frame_age_s").value
        )

        self.frame_queue_size = max(1, int(self.get_parameter("frame_queue_size").value))
        self.resize_changed_frame = bool(
            self.get_parameter("resize_changed_frame").value
        )

        self.require_image_before_folder = bool(
            self.get_parameter("require_image_before_folder").value
        )
        self.required_image_max_age_s = float(
            self.get_parameter("required_image_max_age_s").value
        )

        self.external_mount_point = os.path.expanduser(
            str(self.get_parameter("external_mount_point").value)
        )
        self.local_mount_point = os.path.expanduser(
            str(self.get_parameter("local_mount_point").value)
        )

        self.enable_external_logging = bool(
            self.get_parameter("enable_external_logging").value
        )
        self.enable_local_logging = bool(
            self.get_parameter("enable_local_logging").value
        )
        self.require_external_on_mission = bool(
            self.get_parameter("require_external_on_mission").value
        )
        self.single_target_mode = bool(
            self.get_parameter("single_target_mode").value
        )

        self.create_own_mission_folder_if_missing = bool(
            self.get_parameter("create_own_mission_folder_if_missing").value
        )
        self.mission_folder_wait_sec = float(
            self.get_parameter("mission_folder_wait_sec").value
        )
        self.mission_folder_attach_timeout_sec = float(
            self.get_parameter("mission_folder_attach_timeout_sec").value
        )

        self.mission_gate_topic = str(self.get_parameter("mission_gate_topic").value)
        self.force_record_without_mavros = bool(
            self.get_parameter("force_record_without_mavros").value
        )
        self.stop_on_disarm = bool(self.get_parameter("stop_on_disarm").value)

        self.index_flush_every_n_frames = max(
            1,
            int(self.get_parameter("index_flush_every_n_frames").value),
        )
        self.status_period_s = max(
            1.0,
            float(self.get_parameter("status_period_s").value),
        )

        # ============================================================
        # INTERNAL STATE
        # ============================================================
        self.bridge = CvBridge()
        self.frame_queue = queue.Queue(maxsize=self.frame_queue_size)

        self.latest_packet = None
        self.latest_packet_lock = threading.Lock()

        self.state_received = False
        self.last_connected_state = False
        self.last_armed_state = False
        self.last_flight_mode = "UNKNOWN"

        self.logging_active = False
        self.writer_thread = None
        self.writer_stop_event = threading.Event()

        self.pending_start_request = False
        self.pending_start_state = None
        self.last_waiting_image_warn_time = 0.0

        self.local_timezone = time.tzname[0]
        self.mission_id = None
        self.targets = []

        self.frame_width = None
        self.frame_height = None
        self.segment_index = 0
        self.segment_start_wall = 0.0

        self.rx_frame_seq = 0
        self.last_written_seq = -1
        self.last_write_wall = 0.0

        self.frame_count_in = 0
        self.frame_count_written = 0
        self.frame_count_queue_drop = 0
        self.frame_count_stale_drop = 0
        self.frame_count_duplicate_drop = 0
        self.frame_count_throttle_drop = 0
        self.frame_count_resize = 0
        self.writer_error_count = 0

        self.frame_count_repeated_write = 0
        self.frame_count_no_frame_tick = 0
        self.frame_count_repeat_stale_drop = 0

        self.first_frame_wall = 0.0
        self.last_frame_wall = 0.0
        self.start_record_wall = 0.0
        self.start_record_local_time = None
        self.end_record_local_time = None

        self.last_status_text = ""

        # ============================================================
        # ROS INTERFACES
        # ============================================================
        self.image_sub = self.create_subscription(
            Image,
            self.image_topic,
            self.image_callback,
            self.make_image_qos(),
        )

        self.state_sub = self.create_subscription(
            State,
            self.mission_gate_topic,
            self.mavros_state_callback,
            self.make_state_qos(),
        )

        self.status_timer = self.create_timer(
            self.status_period_s,
            self.status_timer_callback,
        )

        self.pending_start_timer = self.create_timer(
            0.2,
            self.pending_start_timer_callback,
        )

        self.autostart_timer = self.create_timer(
            0.5,
            self.autostart_timer_callback,
        )

        self.set_status(
            f"CA DEBUG VIDEO LOGGER STANDBY | source={self.image_topic} | "
            f"fps={self.output_fps} | constant_output_fps={self.constant_output_fps} | "
            f"require_image_before_folder={self.require_image_before_folder}"
        )

    # ================================================================
    # QOS
    # ================================================================
    def make_image_qos(self):
        if self.image_reliability == "best_effort":
            reliability = ReliabilityPolicy.BEST_EFFORT
        else:
            reliability = ReliabilityPolicy.RELIABLE

        return QoSProfile(
            history=HistoryPolicy.KEEP_LAST,
            depth=self.qos_depth,
            reliability=reliability,
            durability=DurabilityPolicy.VOLATILE,
        )

    def make_state_qos(self):
        return QoSProfile(
            history=HistoryPolicy.KEEP_LAST,
            depth=10,
            reliability=ReliabilityPolicy.BEST_EFFORT,
            durability=DurabilityPolicy.VOLATILE,
        )

    # ================================================================
    # STATUS
    # ================================================================
    def set_status(self, text):
        if text != self.last_status_text:
            self.get_logger().info(text)
            self.last_status_text = text

    def status_timer_callback(self):
        now_wall = time.monotonic()

        if self.last_frame_wall > 0.0:
            image_age = now_wall - self.last_frame_wall
        else:
            image_age = -1.0

        qsize = self.frame_queue.qsize()

        if self.logging_active:
            self.set_status(
                "CA DEBUG VIDEO LOGGER ACTIVE | "
                f"in={self.frame_count_in} "
                f"written={self.frame_count_written} "
                f"repeat_write={self.frame_count_repeated_write} "
                f"qdrop={self.frame_count_queue_drop} "
                f"stale={self.frame_count_stale_drop} "
                f"repeat_stale={self.frame_count_repeat_stale_drop} "
                f"no_frame_tick={self.frame_count_no_frame_tick} "
                f"resize={self.frame_count_resize} "
                f"queue={qsize} "
                f"image_age={image_age:.2f}s"
            )
        else:
            self.set_status(
                "CA DEBUG VIDEO LOGGER STANDBY | "
                f"source={self.image_topic} "
                f"in={self.frame_count_in} "
                f"image_age={image_age:.2f}s "
                f"pending_start={self.pending_start_request}"
            )

        if self.frame_count_in <= 0:
            self.warn_waiting_image_once()

    # ================================================================
    # IMAGE GATE
    # ================================================================
    def is_image_ready_for_folder(self):
        if not self.require_image_before_folder:
            return True

        if self.frame_count_in <= 0:
            return False

        if self.last_frame_wall <= 0.0:
            return False

        image_age = time.monotonic() - self.last_frame_wall

        if image_age > self.required_image_max_age_s:
            return False

        return True

    def warn_waiting_image_once(self):
        now = time.monotonic()

        if now - self.last_waiting_image_warn_time < 2.0:
            return

        self.last_waiting_image_warn_time = now

        if self.frame_count_in <= 0:
            self.get_logger().warning(
                f"Waiting for first valid image from {self.image_topic}. "
                "Mission/video folder will NOT be created yet."
            )
            return

        image_age = now - self.last_frame_wall

        self.get_logger().warning(
            f"Image topic {self.image_topic} is stale. "
            f"last_image_age={image_age:.2f}s, "
            f"limit={self.required_image_max_age_s:.2f}s. "
            "Mission/video folder will NOT be created yet."
        )

    # ================================================================
    # IMAGE CALLBACK
    # ================================================================
    def image_callback(self, msg):
        try:
            frame = self.bridge.imgmsg_to_cv2(msg, desired_encoding="bgr8")
        except Exception as e:
            self.get_logger().error(f"cv_bridge failed: {e}")
            return

        if frame is None:
            return

        now_wall = time.monotonic()

        if self.first_frame_wall <= 0.0:
            self.first_frame_wall = now_wall

        self.last_frame_wall = now_wall
        self.rx_frame_seq += 1
        self.frame_count_in += 1

        source_stamp = self.stamp_to_sec(msg)

        if source_stamp <= 0.0:
            source_stamp = now_wall

        packet = {
            "seq": self.rx_frame_seq,
            "frame": frame.copy(),
            "source_stamp": source_stamp,
            "rx_wall": now_wall,
            "ros_frame_id": str(msg.header.frame_id),
        }

        with self.latest_packet_lock:
            self.latest_packet = packet

        try:
            self.frame_queue.put_nowait(packet)
        except queue.Full:
            try:
                _ = self.frame_queue.get_nowait()
                self.frame_count_queue_drop += 1
            except queue.Empty:
                pass

            try:
                self.frame_queue.put_nowait(packet)
            except queue.Full:
                self.frame_count_queue_drop += 1

    @staticmethod
    def stamp_to_sec(msg):
        try:
            return float(msg.header.stamp.sec) + float(msg.header.stamp.nanosec) * 1e-9
        except Exception:
            return 0.0

    # ================================================================
    # MAVROS STATE / MISSION GATE
    # ================================================================
    def mavros_state_callback(self, msg):
        self.state_received = True
        self.last_connected_state = bool(msg.connected)
        self.last_armed_state = bool(msg.armed)
        self.last_flight_mode = str(msg.mode)

        if self.force_record_without_mavros:
            if self.stop_on_disarm and self.logging_active and not msg.armed:
                self.stop_logging_session(
                    f"disarmed in force mode | mode={msg.mode}, connected={msg.connected}"
                )
            return

        if msg.armed and not self.logging_active:
            if not self.is_image_ready_for_folder():
                self.pending_start_request = True
                self.pending_start_state = msg
                self.warn_waiting_image_once()
                return

            self.start_logging_session(msg)
            return

        if not msg.armed and self.logging_active:
            self.pending_start_request = False
            self.pending_start_state = None

            self.stop_logging_session(
                f"disarmed | mode={msg.mode}, connected={msg.connected}"
            )
            return

        if not msg.armed:
            self.pending_start_request = False
            self.pending_start_state = None

    def autostart_timer_callback(self):
        if not self.force_record_without_mavros:
            return

        if self.logging_active:
            return

        if not self.is_image_ready_for_folder():
            return

        self.start_logging_session(None)

    def pending_start_timer_callback(self):
        if self.logging_active:
            return

        if not self.pending_start_request:
            return

        if not self.force_record_without_mavros:
            if not self.last_armed_state:
                self.pending_start_request = False
                self.pending_start_state = None
                return

        if not self.is_image_ready_for_folder():
            self.warn_waiting_image_once()
            return

        state_msg = self.pending_start_state
        self.pending_start_request = False
        self.pending_start_state = None

        self.start_logging_session(state_msg)

    # ================================================================
    # SESSION START / STOP
    # ================================================================
    def start_logging_session(self, state_msg=None):
        if self.logging_active:
            return

        if not self.is_image_ready_for_folder():
            self.pending_start_request = True
            self.pending_start_state = state_msg
            self.warn_waiting_image_once()
            return

        if self.mission_folder_wait_sec > 0.0:
            time.sleep(self.mission_folder_wait_sec)

        if not self.prepare_output_folder():
            self.get_logger().error(
                "Failed preparing output folder. No video file was created."
            )
            return

        self.frame_width = None
        self.frame_height = None
        self.segment_index = 0
        self.segment_start_wall = 0.0

        self.frame_count_written = 0
        self.frame_count_queue_drop = 0
        self.frame_count_stale_drop = 0
        self.frame_count_duplicate_drop = 0
        self.frame_count_throttle_drop = 0
        self.frame_count_resize = 0
        self.writer_error_count = 0

        self.frame_count_repeated_write = 0
        self.frame_count_no_frame_tick = 0
        self.frame_count_repeat_stale_drop = 0

        self.last_written_seq = -1
        self.last_write_wall = 0.0
        self.start_record_wall = time.monotonic()
        self.start_record_local_time = datetime.now()
        self.end_record_local_time = None

        self.clear_frame_queue()

        for target in self.targets:
            self.write_start_info(target, state_msg)

        self.writer_stop_event.clear()
        self.logging_active = True

        self.writer_thread = threading.Thread(
            target=self.writer_loop,
            name="seano_ca_debug_video_writer_30fps",
            daemon=True,
        )
        self.writer_thread.start()

        self.set_status(
            f"CA DEBUG VIDEO LOGGER ACTIVE | source={self.image_topic} | "
            f"output_fps={self.output_fps}"
        )

    def stop_logging_session(self, reason="stop requested"):
        if not self.logging_active:
            return

        self.logging_active = False
        self.writer_stop_event.set()
        self.end_record_local_time = datetime.now()

        if self.writer_thread is not None:
            try:
                self.writer_thread.join(timeout=3.0)
            except Exception:
                pass

        self.writer_thread = None

        self.close_current_video()

        for target in self.targets:
            self.write_summary_csv(target, reason)
            self.write_end_info(target, reason)

            try:
                if target["frame_csv_file"] is not None:
                    target["frame_csv_file"].flush()
                    target["frame_csv_file"].close()
            except Exception:
                pass

            target["frame_csv_file"] = None
            target["frame_csv_writer"] = None

        try:
            os.sync()
        except Exception:
            pass

        self.targets = []
        self.set_status("CA DEBUG VIDEO LOGGER STANDBY")

    def clear_frame_queue(self):
        while True:
            try:
                self.frame_queue.get_nowait()
            except queue.Empty:
                break

    # ================================================================
    # WRITER LOOP: CONSTANT 30 FPS
    # ================================================================
    def writer_loop(self):
        period = 1.0 / self.output_fps
        next_tick = time.monotonic()

        last_packet = None

        while not self.writer_stop_event.is_set():
            now_wall = time.monotonic()
            sleep_time = next_tick - now_wall

            if sleep_time > 0.0:
                self.writer_stop_event.wait(timeout=min(sleep_time, 0.01))
                continue

            now_wall = time.monotonic()

            latest_from_queue = None

            # Drain queue supaya writer selalu memakai frame terbaru.
            # Ini mencegah video delay karena backlog.
            while True:
                try:
                    latest_from_queue = self.frame_queue.get_nowait()
                except queue.Empty:
                    break

            if latest_from_queue is not None:
                last_packet = latest_from_queue
            else:
                with self.latest_packet_lock:
                    if self.latest_packet is not None:
                        last_packet = self.latest_packet

            if last_packet is None:
                self.frame_count_no_frame_tick += 1
                next_tick += period
                next_tick = self.correct_writer_clock_if_late(period, next_tick)
                continue

            seq = int(last_packet["seq"])
            frame = last_packet["frame"]
            source_stamp = float(last_packet["source_stamp"])
            rx_wall = float(last_packet["rx_wall"])

            frame_age = now_wall - rx_wall
            frame_age_ms = frame_age * 1000.0
            is_repeated_output = seq == self.last_written_seq

            if is_repeated_output and frame_age > self.max_repeat_frame_age_s:
                self.frame_count_repeat_stale_drop += 1
                next_tick += period
                next_tick = self.correct_writer_clock_if_late(period, next_tick)
                continue

            if frame_age > self.max_stale_frame_age_s:
                self.frame_count_stale_drop += 1
                next_tick += period
                next_tick = self.correct_writer_clock_if_late(period, next_tick)
                continue

            if self.write_only_new_frames and is_repeated_output:
                self.frame_count_duplicate_drop += 1
                next_tick += period
                next_tick = self.correct_writer_clock_if_late(period, next_tick)
                continue

            if is_repeated_output:
                if not self.duplicate_last_frame_to_match_fps:
                    self.frame_count_duplicate_drop += 1
                    next_tick += period
                    next_tick = self.correct_writer_clock_if_late(period, next_tick)
                    continue

                self.frame_count_repeated_write += 1
                frame_status = "REPEATED_FRAME"
                note = "Frame source belum update; frame terakhir diulang untuk menjaga output 30 FPS."
            else:
                frame_status = "NEW_FRAME"
                note = "Frame baru dari topic diterima dan ditulis ke video."

            height, width = frame.shape[:2]

            if self.frame_width is None or self.frame_height is None:
                self.frame_width = int(width)
                self.frame_height = int(height)
                self.open_new_video(self.frame_width, self.frame_height)

            if width != self.frame_width or height != self.frame_height:
                if self.resize_changed_frame:
                    frame = cv2.resize(
                        frame,
                        (self.frame_width, self.frame_height),
                        interpolation=cv2.INTER_AREA,
                    )
                    width = self.frame_width
                    height = self.frame_height
                    self.frame_count_resize += 1
                    note = note + " Ukuran frame berubah, lalu di-resize."
                else:
                    self.get_logger().warning(
                        f"Drop frame due size change: got={width}x{height}, "
                        f"expected={self.frame_width}x{self.frame_height}"
                    )
                    next_tick += period
                    next_tick = self.correct_writer_clock_if_late(period, next_tick)
                    continue

            now_unix = time.time()

            if (
                self.segment_seconds > 0.0
                and self.segment_start_wall > 0.0
                and now_unix - self.segment_start_wall >= self.segment_seconds
            ):
                self.open_new_video(self.frame_width, self.frame_height)

            active_writer_count = 0

            for target in self.targets:
                writer = target.get("writer")
                if writer is None:
                    continue

                try:
                    writer.write(frame)
                    active_writer_count += 1
                except Exception as e:
                    self.writer_error_count += 1
                    self.get_logger().error(
                        f"Writer failed for target={target['name']}: {e}"
                    )

            if active_writer_count > 0:
                self.frame_count_written += 1
                self.last_written_seq = seq
                self.last_write_wall = now_wall

                self.write_frame_csv_row(
                    source_seq=seq,
                    width=int(width),
                    height=int(height),
                    source_stamp=source_stamp,
                    rx_wall=rx_wall,
                    frame_age_ms=frame_age_ms,
                    frame_status=frame_status,
                    repeated_output=is_repeated_output,
                    note=note,
                )

            next_tick += period
            next_tick = self.correct_writer_clock_if_late(period, next_tick)

    def correct_writer_clock_if_late(self, period, next_tick):
        now = time.monotonic()

        if next_tick < now - period:
            self.frame_count_throttle_drop += 1
            return now + period

        return next_tick

    # ================================================================
    # VIDEO FILE HANDLING
    # ================================================================
    def open_new_video(self, width, height):
        self.close_current_video()

        self.segment_index += 1
        self.segment_start_wall = time.time()

        if self.segment_seconds > 0.0:
            final_filename = f"ca_debug_video_segment_{self.segment_index:03d}.mp4"
        else:
            final_filename = "ca_debug_mission_video.mp4"

        temp_filename = final_filename.replace(".mp4", ".recording.mp4")
        fourcc = cv2.VideoWriter_fourcc(*self.codec)

        for target in self.targets:
            video_dir = target["video_dir"]
            temp_path = os.path.join(video_dir, temp_filename)
            final_path = os.path.join(video_dir, final_filename)

            target["writer"] = None
            target["temp_path"] = temp_path
            target["final_path"] = final_path
            target["current_filename"] = final_filename

            try:
                writer = cv2.VideoWriter(
                    temp_path,
                    fourcc,
                    self.output_fps,
                    (int(width), int(height)),
                )

                if not writer.isOpened():
                    self.writer_error_count += 1
                    self.get_logger().error(
                        f"Failed opening video writer: {temp_path}"
                    )
                    continue

                target["writer"] = writer

            except Exception as e:
                self.writer_error_count += 1
                self.get_logger().error(f"Exception opening writer: {e}")

    def close_current_video(self):
        for target in self.targets:
            writer = target.get("writer")
            temp_path = target.get("temp_path")
            final_path = target.get("final_path")

            if writer is not None:
                try:
                    writer.release()
                except Exception:
                    pass

            target["writer"] = None

            if temp_path is not None and final_path is not None:
                try:
                    if os.path.exists(temp_path):
                        os.replace(temp_path, final_path)
                except Exception as e:
                    self.writer_error_count += 1
                    self.get_logger().error(
                        f"Failed finalizing video file: {e}"
                    )

            target["temp_path"] = None
            target["final_path"] = None
            target["current_filename"] = None

        try:
            os.sync()
        except Exception:
            pass

    # ================================================================
    # CSV FILES
    # ================================================================
    def get_frame_csv_header(self):
        return [
            "waktu_lokal",
            "mission_id",
            "target_penyimpanan",
            "topic_sumber",
            "nama_file_video",
            "segmen_video_ke",
            "nomor_frame_video",
            "setting_output_fps",
            "status_frame",
            "apakah_frame_diulang",
            "nomor_frame_dari_topic",
            "umur_frame_ms",
            "waktu_ros_frame_detik",
            "waktu_diterima_logger_monotonic_detik",
            "lebar_frame_px",
            "tinggi_frame_px",
            "jumlah_antrian_frame_saat_ditulis",
            "jumlah_error_writer_saat_ditulis",
            "catatan",
        ]

    def get_summary_csv_header(self):
        return [
            "waktu_mulai_lokal",
            "waktu_selesai_lokal",
            "alasan_berhenti",
            "mission_id",
            "target_penyimpanan",
            "topic_sumber",
            "setting_output_fps",
            "durasi_recording_detik",
            "total_frame_masuk_dari_topic",
            "total_frame_video_ditulis",
            "total_frame_diulang",
            "persentase_frame_diulang",
            "queue_drop_frames",
            "stale_drop_frames",
            "duplicate_drop_frames",
            "no_frame_tick_count",
            "repeat_stale_drop_count",
            "resize_count",
            "writer_error_count",
            "effective_written_fps",
            "folder_video",
        ]

    def write_frame_csv_row(
        self,
        source_seq,
        width,
        height,
        source_stamp,
        rx_wall,
        frame_age_ms,
        frame_status,
        repeated_output,
        note,
    ):
        local_timestamp = self.get_local_timestamp()
        queue_size = self.frame_queue.qsize()

        for target in self.targets:
            writer = target.get("frame_csv_writer")

            if writer is None:
                continue

            filename = target.get("current_filename") or ""

            try:
                writer.writerow([
                    local_timestamp,
                    self.mission_id or "",
                    target.get("name", ""),
                    self.image_topic,
                    filename,
                    self.segment_index,
                    self.frame_count_written,
                    f"{self.output_fps:.2f}",
                    frame_status,
                    "YA" if repeated_output else "TIDAK",
                    source_seq,
                    f"{frame_age_ms:.2f}",
                    f"{source_stamp:.6f}",
                    f"{rx_wall:.6f}",
                    width,
                    height,
                    queue_size,
                    self.writer_error_count,
                    note,
                ])

                if self.frame_count_written % self.index_flush_every_n_frames == 0:
                    target["frame_csv_file"].flush()

            except Exception:
                pass

    def write_summary_csv(self, target, reason):
        summary_path = target.get("summary_csv_path")
        if summary_path is None:
            return

        record_duration = 0.0
        if self.start_record_wall > 0.0:
            record_duration = time.monotonic() - self.start_record_wall

        effective_fps = 0.0
        if record_duration > 0.0:
            effective_fps = self.frame_count_written / record_duration

        repeated_percent = 0.0
        if self.frame_count_written > 0:
            repeated_percent = (
                self.frame_count_repeated_write / self.frame_count_written
            ) * 100.0

        start_time = ""
        if self.start_record_local_time is not None:
            start_time = self.start_record_local_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        end_time = ""
        if self.end_record_local_time is not None:
            end_time = self.end_record_local_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        try:
            with open(summary_path, "w", newline="") as summary_file:
                writer = csv.writer(summary_file)
                writer.writerow(self.get_summary_csv_header())
                writer.writerow([
                    start_time,
                    end_time,
                    reason,
                    self.mission_id or "",
                    target.get("name", ""),
                    self.image_topic,
                    f"{self.output_fps:.2f}",
                    f"{record_duration:.3f}",
                    self.frame_count_in,
                    self.frame_count_written,
                    self.frame_count_repeated_write,
                    f"{repeated_percent:.2f}",
                    self.frame_count_queue_drop,
                    self.frame_count_stale_drop,
                    self.frame_count_duplicate_drop,
                    self.frame_count_no_frame_tick,
                    self.frame_count_repeat_stale_drop,
                    self.frame_count_resize,
                    self.writer_error_count,
                    f"{effective_fps:.3f}",
                    target.get("video_dir", ""),
                ])
        except Exception as e:
            self.get_logger().error(f"Failed writing summary CSV: {e}")

    # ================================================================
    # OUTPUT FOLDER
    # ================================================================
    def prepare_output_folder(self):
        deadline = time.time() + max(0.0, self.mission_folder_attach_timeout_sec)

        while time.time() <= deadline:
            if self.try_attach_output_folder_once():
                return True

            time.sleep(0.2)

        if self.create_own_mission_folder_if_missing:
            return self.create_and_attach_own_mission_folder()

        return False

    def get_today_roots(self):
        now = datetime.now()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")

        roots = []

        if self.enable_external_logging:
            roots.append(
                (
                    "external",
                    os.path.join(
                        self.external_mount_point,
                        "SEANO_MISSIONS",
                        year,
                        month,
                        day,
                    ),
                )
            )

        if self.enable_local_logging:
            roots.append(
                (
                    "local",
                    os.path.join(
                        self.local_mount_point,
                        year,
                        month,
                        day,
                    ),
                )
            )

        if self.single_target_mode:
            roots = roots[:1]

        return roots

    def try_attach_output_folder_once(self):
        roots = self.get_today_roots()
        latest_candidates = []

        for target_name, day_root in roots:
            mission_path = self.find_latest_mission_folder(day_root)

            if mission_path is None:
                continue

            try:
                mtime = os.path.getmtime(mission_path)
            except OSError:
                mtime = 0.0

            latest_candidates.append((mtime, mission_path))

        if len(latest_candidates) == 0:
            return False

        latest_candidates.sort(key=lambda item: item[0], reverse=True)
        selected_mission_id = os.path.basename(latest_candidates[0][1])

        self.targets = []

        for target_name, day_root in roots:
            mission_base_path = os.path.join(day_root, selected_mission_id)

            if not os.path.isdir(mission_base_path):
                continue

            try:
                self.add_logging_target(mission_base_path, target_name)
            except Exception as e:
                self.get_logger().error(
                    f"Failed attach target={target_name}: {e}"
                )

        if self.require_external_on_mission:
            has_external = any(target["name"] == "external" for target in self.targets)

            if not has_external:
                self.targets = []
                return False

        if len(self.targets) == 0:
            return False

        self.mission_id = selected_mission_id
        return True

    @staticmethod
    def find_latest_mission_folder(day_root):
        if not os.path.isdir(day_root):
            return None

        candidates = []

        for name in os.listdir(day_root):
            path = os.path.join(day_root, name)

            if not os.path.isdir(path):
                continue

            if not name.startswith("MISSION_START_"):
                continue

            try:
                mtime = os.path.getmtime(path)
            except OSError:
                continue

            candidates.append((mtime, path))

        if len(candidates) == 0:
            return None

        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    def create_and_attach_own_mission_folder(self):
        now = datetime.now()
        mission_id = now.strftime(f"MISSION_START_%H-%M-%S_{self.local_timezone}")

        self.targets = []

        for target_name, day_root in self.get_today_roots():
            mission_base_path = os.path.join(day_root, mission_id)

            try:
                os.makedirs(mission_base_path, exist_ok=True)
                self.add_logging_target(mission_base_path, target_name)
            except Exception as e:
                self.get_logger().error(
                    f"Failed creating target={target_name}: {e}"
                )

        if len(self.targets) == 0:
            return False

        self.mission_id = mission_id
        self.get_logger().warning(
            f"Created own mission folder for CA debug video: {mission_id}"
        )

        return True

    def add_logging_target(self, base_path, target_name):
        video_dir = os.path.join(base_path, "video")
        os.makedirs(video_dir, exist_ok=True)

        frame_csv_path = os.path.join(video_dir, "ca_debug_video_frames.csv")
        summary_csv_path = os.path.join(video_dir, "ca_debug_video_summary.csv")

        frame_csv_file = open(frame_csv_path, "w", newline="", buffering=1)
        frame_csv_writer = csv.writer(frame_csv_file)
        frame_csv_writer.writerow(self.get_frame_csv_header())

        target = {
            "name": target_name,
            "base_path": base_path,
            "video_dir": video_dir,

            "frame_csv_path": frame_csv_path,
            "summary_csv_path": summary_csv_path,
            "frame_csv_file": frame_csv_file,
            "frame_csv_writer": frame_csv_writer,

            "writer": None,
            "temp_path": None,
            "final_path": None,
            "current_filename": None,
        }

        self.targets.append(target)

    # ================================================================
    # INFO FILE
    # ================================================================
    def write_start_info(self, target, state_msg=None):
        info_path = os.path.join(target["video_dir"], "ca_debug_video_info.txt")

        with open(info_path, "w") as f:
            f.write("=== CA DEBUG VIDEO LOGGER START ===\n")
            f.write(f"Start Time: {datetime.now()}\n")
            f.write("Platform: SEANO USV\n")
            f.write("Logger Type: ROS Image Topic Constant 30 FPS Video Logger\n")
            f.write(f"Source Image Topic: {self.image_topic}\n")
            f.write(f"Image Reliability: {self.image_reliability}\n")
            f.write(f"QoS Depth: {self.qos_depth}\n")
            f.write(f"Target Name: {target['name']}\n")
            f.write(f"Mission Folder: {target['base_path']}\n")
            f.write(f"Video Dir: {target['video_dir']}\n")
            f.write(f"Frame CSV Path: {target['frame_csv_path']}\n")
            f.write(f"Summary CSV Path: {target['summary_csv_path']}\n")
            f.write(f"Output FPS: {self.output_fps}\n")
            f.write(f"Codec: {self.codec}\n")
            f.write(f"Segment Seconds: {self.segment_seconds}\n")
            f.write(f"Constant Output FPS: {self.constant_output_fps}\n")
            f.write(f"Duplicate Last Frame To Match FPS: {self.duplicate_last_frame_to_match_fps}\n")
            f.write(f"Write Only New Frames: {self.write_only_new_frames}\n")
            f.write(f"Max Repeat Frame Age S: {self.max_repeat_frame_age_s}\n")
            f.write(f"Max Stale Frame Age S: {self.max_stale_frame_age_s}\n")
            f.write(f"Frame Queue Size: {self.frame_queue_size}\n")
            f.write(f"Require Image Before Folder: {self.require_image_before_folder}\n")
            f.write(f"Required Image Max Age S: {self.required_image_max_age_s}\n")
            f.write(f"Force Record Without MAVROS: {self.force_record_without_mavros}\n")
            f.write(f"Create Own Mission Folder If Missing: {self.create_own_mission_folder_if_missing}\n")

            if state_msg is not None:
                f.write(f"Start MAVROS Connected: {state_msg.connected}\n")
                f.write(f"Start MAVROS Armed: {state_msg.armed}\n")
                f.write(f"Start MAVROS Mode: {state_msg.mode}\n")
            else:
                f.write("Start MAVROS Connected: UNKNOWN\n")
                f.write("Start MAVROS Armed: UNKNOWN\n")
                f.write("Start MAVROS Mode: UNKNOWN\n")

            f.write("\n=== CSV COLUMN NOTES ===\n")
            f.write("status_frame = NEW_FRAME jika frame baru dari topic, REPEATED_FRAME jika frame diulang.\n")
            f.write("apakah_frame_diulang = YA jika frame output video adalah pengulangan frame sebelumnya.\n")
            f.write("umur_frame_ms = selisih waktu antara frame diterima logger dan waktu frame ditulis ke video.\n")
            f.write("persentase_frame_diulang pada summary menunjukkan seberapa besar video bergantung pada frame repeat.\n")

    def write_end_info(self, target, reason):
        info_path = os.path.join(target["video_dir"], "ca_debug_video_info.txt")

        record_duration = 0.0

        if self.start_record_wall > 0.0:
            record_duration = time.monotonic() - self.start_record_wall

        effective_fps = 0.0
        if record_duration > 0.0:
            effective_fps = self.frame_count_written / record_duration

        repeated_percent = 0.0
        if self.frame_count_written > 0:
            repeated_percent = (
                self.frame_count_repeated_write / self.frame_count_written
            ) * 100.0

        with open(info_path, "a") as f:
            f.write("\n=== CA DEBUG VIDEO LOGGER END ===\n")
            f.write(f"End Time: {datetime.now()}\n")
            f.write(f"Stop Reason: {reason}\n")
            f.write(f"Record Duration S: {record_duration:.3f}\n")
            f.write(f"Total Input Frames From Topic: {self.frame_count_in}\n")
            f.write(f"Total Written Video Frames: {self.frame_count_written}\n")
            f.write(f"Repeated Written Frames: {self.frame_count_repeated_write}\n")
            f.write(f"Repeated Written Percent: {repeated_percent:.2f}%\n")
            f.write(f"Queue Drop Frames: {self.frame_count_queue_drop}\n")
            f.write(f"Stale Drop Frames: {self.frame_count_stale_drop}\n")
            f.write(f"Duplicate Drop Frames: {self.frame_count_duplicate_drop}\n")
            f.write(f"Throttle Drop Frames: {self.frame_count_throttle_drop}\n")
            f.write(f"No Frame Tick Count: {self.frame_count_no_frame_tick}\n")
            f.write(f"Repeat Stale Drop Count: {self.frame_count_repeat_stale_drop}\n")
            f.write(f"Resize Count: {self.frame_count_resize}\n")
            f.write(f"Writer Error Count: {self.writer_error_count}\n")
            f.write(f"Effective Written FPS: {effective_fps:.3f}\n")
            f.write(f"End MAVROS Connected: {self.last_connected_state}\n")
            f.write(f"End MAVROS Armed: {self.last_armed_state}\n")
            f.write(f"End MAVROS Mode: {self.last_flight_mode}\n")

    # ================================================================
    # UTILITIES
    # ================================================================
    @staticmethod
    def get_local_timestamp():
        now = datetime.now()
        return now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def destroy_node(self):
        if self.logging_active:
            self.stop_logging_session("node shutdown")
        else:
            self.close_current_video()

        for target in self.targets:
            try:
                if target["frame_csv_file"] is not None:
                    target["frame_csv_file"].flush()
                    target["frame_csv_file"].close()
            except Exception:
                pass

        try:
            os.sync()
        except Exception:
            pass

        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)
    node = SeanoCaDebugVideoLogger()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()