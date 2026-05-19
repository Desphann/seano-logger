#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
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

from sensor_msgs.msg import CompressedImage
from mavros_msgs.msg import State


class SeanoVideoLogger(Node):
    def __init__(self):
        super().__init__("video_logger_node")

        # ============================================================
        # CAMERA CONFIG
        # ============================================================
        self.declare_parameter("camera_device", "/dev/video0")
        self.declare_parameter("camera_width", 640)
        self.declare_parameter("camera_height", 480)

        # Kamera kamu support maksimum 30 FPS.
        self.declare_parameter("camera_fps", 30.0)
        self.declare_parameter("use_mjpg", True)

        # Debug preview default OFF supaya recording lebih ringan.
        # Tidak ada HUD / overlay di program ini.
        self.declare_parameter("publish_debug_compressed", False)
        self.declare_parameter("debug_topic", "/seano/camera/debug/compressed")
        self.declare_parameter("debug_fps", 2.0)
        self.declare_parameter("debug_jpeg_quality", 60)

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

        # False = video masuk ke external dan local Documents,
        # tetapi tetap di mission folder existing yang dibuat logger utama.
        self.declare_parameter("single_target_mode", False)

        # Jangan bikin mission folder sendiri agar tidak pecah folder.
        self.declare_parameter("create_own_mission_folder_if_missing", False)

        # Tunggu logger utama membuat folder mission setelah armed.
        self.declare_parameter("mission_folder_wait_sec", 0.8)
        self.declare_parameter("mission_folder_attach_timeout_sec", 5.0)

        # ============================================================
        # VIDEO CONFIG
        # ============================================================
        self.declare_parameter("output_fps", 30.0)
        self.declare_parameter("codec", "mp4v")

        # 0.0 = satu file mission_video.mp4 per mission.
        self.declare_parameter("segment_seconds", 0.0)

        # Jangan flush CSV setiap frame karena bisa bikin stutter.
        self.declare_parameter("index_flush_every_n_frames", 120)

        self.declare_parameter("black_frame_until_camera_ready", True)

        # ============================================================
        # MISSION GATE CONFIG
        # ============================================================
        self.declare_parameter("mission_gate_topic", "/mavros/state")

        # Tetap dideklarasikan supaya launch lama tidak error.
        # Sengaja tidak dipakai. Logging tetap wajib MAVROS armed=True.
        self.declare_parameter("force_record_without_mavros", False)

        # ============================================================
        # LOAD PARAMETERS
        # ============================================================
        self.camera_device = str(self.get_parameter("camera_device").value)
        self.camera_width = int(self.get_parameter("camera_width").value)
        self.camera_height = int(self.get_parameter("camera_height").value)
        self.camera_fps = float(self.get_parameter("camera_fps").value)
        self.use_mjpg = bool(self.get_parameter("use_mjpg").value)

        self.publish_debug_compressed = bool(
            self.get_parameter("publish_debug_compressed").value
        )
        self.debug_topic = str(self.get_parameter("debug_topic").value)
        self.debug_fps = float(self.get_parameter("debug_fps").value)
        self.debug_jpeg_quality = int(self.get_parameter("debug_jpeg_quality").value)
        self.debug_jpeg_quality = max(10, min(95, self.debug_jpeg_quality))

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

        self.output_fps = float(self.get_parameter("output_fps").value)
        self.codec = str(self.get_parameter("codec").value)
        self.segment_seconds = float(self.get_parameter("segment_seconds").value)

        self.index_flush_every_n_frames = max(
            1,
            int(self.get_parameter("index_flush_every_n_frames").value),
        )

        self.black_frame_until_camera_ready = bool(
            self.get_parameter("black_frame_until_camera_ready").value
        )

        self.mission_gate_topic = str(self.get_parameter("mission_gate_topic").value)
        self.force_record_without_mavros = bool(
            self.get_parameter("force_record_without_mavros").value
        )

        if self.camera_fps <= 0.0:
            self.camera_fps = 30.0

        if self.output_fps <= 0.0:
            self.output_fps = 30.0

        if self.debug_fps <= 0.0:
            self.debug_fps = 2.0

        if len(self.codec) != 4:
            self.codec = "mp4v"

        # ============================================================
        # INTERNAL STATE
        # ============================================================
        self.cap = None

        self.capture_thread = None
        self.capture_running = False

        self.frame_lock = threading.Lock()
        self.latest_frame = None
        self.latest_frame_time = 0.0

        self.writer_thread = None
        self.writer_stop_event = threading.Event()

        self.logging_active = False

        self.last_armed_state = False
        self.last_connected_state = False
        self.last_flight_mode = "UNKNOWN"
        self.state_received = False

        self.local_timezone = time.tzname[0]
        self.mission_id = None

        self.targets = []

        self.segment_index = 0
        self.segment_start_wall = 0.0

        self.frame_width = None
        self.frame_height = None

        self.frame_count_in = 0
        self.frame_count_written = 0
        self.frame_count_duplicated = 0
        self.frame_count_black = 0
        self.writer_late_count = 0

        self.last_written_source_time = 0.0

        self.last_status_text = None
        self.last_status_print_time = 0.0

        # ============================================================
        # OPEN CAMERA + START CAPTURE THREAD
        # ============================================================
        self.open_camera()
        self.start_capture_thread()

        # ============================================================
        # ROS INTERFACES
        # ============================================================
        self.state_sub = self.create_subscription(
            State,
            self.mission_gate_topic,
            self.mavros_state_callback,
            self.make_state_qos(),
        )

        self.debug_pub = None
        if self.publish_debug_compressed:
            self.debug_pub = self.create_publisher(
                CompressedImage,
                self.debug_topic,
                self.make_debug_qos(),
            )

        self.debug_timer = self.create_timer(
            1.0 / self.debug_fps,
            self.debug_timer_callback,
        )

        self.watchdog_timer = self.create_timer(10.0, self.watchdog_callback)

        self.set_status("VIDEO LOGGER STANDBY")

    # ================================================================
    # MINIMAL STATUS LOGGING
    # ================================================================
    def set_status(self, text):
        if text != self.last_status_text:
            self.get_logger().info(text)
            self.last_status_text = text
            self.last_status_print_time = time.time()

    def warn_once_per_interval(self, text, interval_sec=30.0):
        now = time.time()
        if now - self.last_status_print_time >= interval_sec:
            self.get_logger().warning(text)
            self.last_status_print_time = now

    # ================================================================
    # CAMERA
    # ================================================================
    def parse_camera_device(self):
        device_str = str(self.camera_device).strip()

        if device_str.isdigit():
            return int(device_str)

        return device_str

    def open_camera(self):
        device = self.parse_camera_device()

        self.cap = cv2.VideoCapture(device, cv2.CAP_V4L2)

        if not self.cap.isOpened():
            self.get_logger().fatal(
                f"CAMERA ERROR: failed opening camera device={device}"
            )
            raise RuntimeError(f"Failed opening camera device={device}")

        try:
            self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        except Exception:
            pass

        if self.use_mjpg:
            self.cap.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*"MJPG"))

        self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, float(self.camera_width))
        self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, float(self.camera_height))
        self.cap.set(cv2.CAP_PROP_FPS, float(self.camera_fps))

    def start_capture_thread(self):
        self.capture_running = True

        self.capture_thread = threading.Thread(
            target=self.capture_loop,
            name="seano_camera_capture",
            daemon=True,
        )
        self.capture_thread.start()

    def capture_loop(self):
        while self.capture_running:
            if self.cap is None or not self.cap.isOpened():
                time.sleep(0.1)
                continue

            ret, frame = self.cap.read()

            if not ret or frame is None:
                time.sleep(0.005)
                continue

            now = time.monotonic()

            with self.frame_lock:
                self.latest_frame = frame
                self.latest_frame_time = now
                self.frame_count_in += 1

    def get_latest_frame_copy(self):
        with self.frame_lock:
            if self.latest_frame is None:
                return None, 0.0

            return self.latest_frame.copy(), self.latest_frame_time

    def make_black_frame(self):
        return cv2.UMat(self.camera_height, self.camera_width, cv2.CV_8UC3).get()

    # ================================================================
    # MISSION FOLDER ATTACHMENT
    # ================================================================
    def prepare_output_folder_from_existing_mission(self):
        deadline = time.time() + max(0.0, self.mission_folder_attach_timeout_sec)

        while time.time() <= deadline:
            if self.try_attach_output_folder_once():
                return True

            time.sleep(0.2)

        self.get_logger().error(
            "VIDEO LOGGER ERROR: no existing mission folder found"
        )
        return False

    def try_attach_output_folder_once(self):
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

        latest_candidates = []

        for target_name, day_root in roots:
            mission_path = self.find_latest_mission_folder(day_root)

            if mission_path is not None:
                try:
                    mtime = os.path.getmtime(mission_path)
                except OSError:
                    mtime = 0.0

                latest_candidates.append((mtime, mission_path))

        if len(latest_candidates) == 0:
            if self.create_own_mission_folder_if_missing:
                return self.create_and_attach_own_mission_folder(roots)
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
                    f"VIDEO LOGGER ERROR: failed attach target={target_name}: {e}"
                )

        if self.require_external_on_mission:
            has_external = any(target["name"] == "external" for target in self.targets)

            if not has_external:
                return False

        if len(self.targets) == 0:
            return False

        self.mission_id = selected_mission_id
        return True

    def find_latest_mission_folder(self, day_root):
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

    def create_and_attach_own_mission_folder(self, roots):
        now = datetime.now()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        mission_id = now.strftime(f"MISSION_START_%H-%M-%S_{self.local_timezone}")

        self.targets = []

        for target_name, day_root in roots:
            mission_base_path = os.path.join(day_root, mission_id)

            try:
                os.makedirs(mission_base_path, exist_ok=True)
                self.add_logging_target(mission_base_path, target_name)
            except Exception:
                pass

        if len(self.targets) == 0:
            return False

        self.mission_id = mission_id
        self.get_logger().warning(
            "VIDEO LOGGER WARNING: created own mission folder"
        )
        return True

    def add_logging_target(self, base_path, target_name):
        video_dir = os.path.join(base_path, "video")
        os.makedirs(video_dir, exist_ok=True)

        index_path = os.path.join(video_dir, "video_frames.csv")
        index_file = open(index_path, "w", buffering=1)

        index_file.write(
            "local_timestamp,"
            "segment_index,"
            "frame_index,"
            "width,"
            "height,"
            "filename,"
            "source_frame_time,"
            "duplicated,"
            "black_frame\n"
        )

        target = {
            "name": target_name,
            "base_path": base_path,
            "video_dir": video_dir,
            "index_path": index_path,
            "index_file": index_file,
            "writer": None,
            "temp_path": None,
            "final_path": None,
            "current_filename": None,
        }

        self.targets.append(target)

    def update_start_info(self, target, state_msg=None):
        info_path = os.path.join(target["video_dir"], "video_info.txt")

        with open(info_path, "w") as f:
            f.write(f"Video Logger Start Time: {datetime.now()}\n")
            f.write("Platform: SEANO USV\n")
            f.write("Logger Type: Smooth Full-Duration Direct Camera Video Logger\n")
            f.write(f"Target Name: {target['name']}\n")
            f.write(f"Mission Folder: {target['base_path']}\n")
            f.write(f"Video Dir: {target['video_dir']}\n")
            f.write(f"Camera Device: {self.camera_device}\n")
            f.write(f"Camera Width: {self.camera_width}\n")
            f.write(f"Camera Height: {self.camera_height}\n")
            f.write(f"Camera FPS Request: {self.camera_fps}\n")
            f.write(f"Output FPS: {self.output_fps}\n")
            f.write(f"Codec: {self.codec}\n")
            f.write(f"Segment Seconds: {self.segment_seconds}\n")

            if state_msg is not None:
                f.write(f"Start MAVROS Connected: {state_msg.connected}\n")
                f.write(f"Start MAVROS Armed: {state_msg.armed}\n")
                f.write(f"Start MAVROS Mode: {state_msg.mode}\n")
            else:
                f.write("Start MAVROS Connected: UNKNOWN\n")
                f.write("Start MAVROS Armed: UNKNOWN\n")
                f.write("Start MAVROS Mode: UNKNOWN\n")

    # ================================================================
    # DEBUG COMPRESSED PREVIEW
    # ================================================================
    def debug_timer_callback(self):
        if not self.publish_debug_compressed or self.debug_pub is None:
            return

        frame, _ = self.get_latest_frame_copy()

        if frame is None:
            return

        encode_param = [
            int(cv2.IMWRITE_JPEG_QUALITY),
            self.debug_jpeg_quality,
        ]

        ok, encoded = cv2.imencode(".jpg", frame, encode_param)

        if not ok:
            return

        msg = CompressedImage()
        msg.header.stamp = self.get_clock().now().to_msg()
        msg.header.frame_id = "seano_camera"
        msg.format = "jpeg"
        msg.data = encoded.tobytes()

        self.debug_pub.publish(msg)

    # ================================================================
    # QOS
    # ================================================================
    def make_state_qos(self):
        return QoSProfile(
            history=HistoryPolicy.KEEP_LAST,
            depth=10,
            reliability=ReliabilityPolicy.BEST_EFFORT,
            durability=DurabilityPolicy.VOLATILE,
        )

    def make_debug_qos(self):
        return QoSProfile(
            history=HistoryPolicy.KEEP_LAST,
            depth=2,
            reliability=ReliabilityPolicy.BEST_EFFORT,
            durability=DurabilityPolicy.VOLATILE,
        )

    # ================================================================
    # MAVROS STATE / MISSION GATE
    # ================================================================
    def mavros_state_callback(self, msg):
        self.state_received = True

        self.last_connected_state = msg.connected
        self.last_armed_state = msg.armed
        self.last_flight_mode = msg.mode

        if msg.armed and not self.logging_active:
            self.start_logging_session(msg)

        elif not msg.armed and self.logging_active:
            reason = (
                f"mode={msg.mode}, "
                f"armed={msg.armed}, "
                f"connected={msg.connected}"
            )
            self.stop_logging_session(reason)

    def watchdog_callback(self):
        if self.logging_active:
            self.set_status("VIDEO LOGGER ACTIVE")
        else:
            self.set_status("VIDEO LOGGER STANDBY")

        if not self.state_received:
            self.warn_once_per_interval(
                "VIDEO LOGGER STANDBY: waiting MAVROS state",
                interval_sec=30.0,
            )

    # ================================================================
    # LOGGING SESSION
    # ================================================================
    def start_logging_session(self, state_msg=None):
        if self.logging_active:
            return

        if self.mission_folder_wait_sec > 0.0:
            time.sleep(self.mission_folder_wait_sec)

        if not self.prepare_output_folder_from_existing_mission():
            self.set_status("VIDEO LOGGER STANDBY")
            return

        self.frame_count_written = 0
        self.frame_count_duplicated = 0
        self.frame_count_black = 0
        self.writer_late_count = 0
        self.last_written_source_time = 0.0
        self.frame_width = None
        self.frame_height = None
        self.segment_index = 0
        self.segment_start_wall = 0.0

        for target in self.targets:
            self.update_start_info(target, state_msg)

        self.writer_stop_event.clear()
        self.logging_active = True

        self.writer_thread = threading.Thread(
            target=self.writer_loop,
            name="seano_video_writer",
            daemon=True,
        )
        self.writer_thread.start()

        self.set_status("VIDEO LOGGER ACTIVE")

    # ================================================================
    # FIXED-RATE VIDEO WRITER
    # ================================================================
    def writer_loop(self):
        period = 1.0 / self.output_fps
        next_write_time = time.monotonic()

        while not self.writer_stop_event.is_set():
            now = time.monotonic()
            sleep_time = next_write_time - now

            if sleep_time > 0.0:
                self.writer_stop_event.wait(timeout=min(sleep_time, 0.01))
                continue

            if now - next_write_time > period * 3.0:
                self.writer_late_count += 1
                next_write_time = now

            frame, source_time = self.get_latest_frame_copy()
            is_black = False
            is_duplicated = False

            if frame is None:
                if not self.black_frame_until_camera_ready:
                    next_write_time += period
                    continue

                frame = self.make_black_frame()
                source_time = 0.0
                is_black = True
                self.frame_count_black += 1

            if source_time > 0.0 and source_time == self.last_written_source_time:
                is_duplicated = True
                self.frame_count_duplicated += 1

            self.last_written_source_time = source_time

            height, width = frame.shape[:2]
            self.write_video_frame(
                frame=frame,
                width=width,
                height=height,
                source_time=source_time,
                is_duplicated=is_duplicated,
                is_black=is_black,
            )

            next_write_time += period

    def write_video_frame(
        self,
        frame,
        width,
        height,
        source_time,
        is_duplicated,
        is_black,
    ):
        if self.frame_width is None or self.frame_height is None:
            self.frame_width = width
            self.frame_height = height
            self.open_new_video(width, height)

        if width != self.frame_width or height != self.frame_height:
            frame = cv2.resize(
                frame,
                (self.frame_width, self.frame_height),
                interpolation=cv2.INTER_AREA,
            )

            width = self.frame_width
            height = self.frame_height

        now = time.time()

        if (
            self.segment_seconds > 0.0
            and self.segment_start_wall > 0.0
            and now - self.segment_start_wall >= self.segment_seconds
        ):
            self.open_new_video(width, height)

        active_writer_count = 0

        for target in self.targets:
            writer = target["writer"]

            if writer is None:
                continue

            try:
                writer.write(frame)
                active_writer_count += 1
            except Exception as e:
                self.get_logger().error(
                    f"VIDEO LOGGER ERROR: writer failed for {target['name']}: {e}"
                )

        if active_writer_count == 0:
            return

        self.frame_count_written += 1

        local_timestamp = self.get_local_timestamp()

        for target in self.targets:
            index_file = target["index_file"]

            if index_file is None:
                continue

            filename = target["current_filename"]
            if filename is None:
                filename = ""

            try:
                index_file.write(
                    f"{local_timestamp},"
                    f"{self.segment_index},"
                    f"{self.frame_count_written},"
                    f"{width},"
                    f"{height},"
                    f"{filename},"
                    f"{source_time:.6f},"
                    f"{int(is_duplicated)},"
                    f"{int(is_black)}\n"
                )

                if self.frame_count_written % self.index_flush_every_n_frames == 0:
                    index_file.flush()

            except Exception:
                pass

    def open_new_video(self, width, height):
        self.close_current_video()

        self.segment_index += 1
        self.segment_start_wall = time.time()

        if self.segment_seconds > 0.0:
            final_filename = f"video_segment_{self.segment_index:03d}.mp4"
        else:
            final_filename = "mission_video.mp4"

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
                    self.get_logger().error(
                        f"VIDEO LOGGER ERROR: failed opening writer {temp_path}"
                    )
                    target["writer"] = None
                    continue

                target["writer"] = writer

            except Exception as e:
                self.get_logger().error(
                    f"VIDEO LOGGER ERROR: exception opening writer: {e}"
                )
                target["writer"] = None

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
                    self.get_logger().error(
                        f"VIDEO LOGGER ERROR: failed finalizing video: {e}"
                    )

            target["temp_path"] = None
            target["final_path"] = None
            target["current_filename"] = None

        try:
            os.sync()
        except AttributeError:
            pass

    def stop_logging_session(self, reason="vehicle disarmed"):
        if not self.logging_active:
            return

        self.logging_active = False
        self.writer_stop_event.set()

        if self.writer_thread is not None:
            try:
                self.writer_thread.join(timeout=3.0)
            except Exception:
                pass

            self.writer_thread = None

        self.close_current_video()

        stop_time_obj = datetime.now()

        for target in self.targets:
            try:
                info_path = os.path.join(target["video_dir"], "video_info.txt")

                with open(info_path, "a") as f:
                    f.write("\n=== RECORDING END ===\n")
                    f.write(f"End Time: {stop_time_obj}\n")
                    f.write(f"Stop Reason: {reason}\n")
                    f.write(f"Total Input Frames: {self.frame_count_in}\n")
                    f.write(f"Total Written Frames: {self.frame_count_written}\n")
                    f.write(f"Duplicated Written Frames: {self.frame_count_duplicated}\n")
                    f.write(f"Black Written Frames: {self.frame_count_black}\n")
                    f.write(f"Writer Late Count: {self.writer_late_count}\n")
                    f.write(f"End MAVROS Connected: {self.last_connected_state}\n")
                    f.write(f"End MAVROS Armed: {self.last_armed_state}\n")
                    f.write(f"End MAVROS Mode: {self.last_flight_mode}\n")

            except Exception:
                pass

            try:
                if target["index_file"] is not None:
                    target["index_file"].flush()
                    target["index_file"].close()
            except Exception:
                pass

            target["index_file"] = None

        try:
            os.sync()
        except AttributeError:
            pass

        self.targets = []
        self.set_status("VIDEO LOGGER STANDBY")

    # ================================================================
    # UTILITIES
    # ================================================================
    def get_local_timestamp(self):
        now = datetime.now()
        return now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def destroy_node(self):
        if self.logging_active:
            self.stop_logging_session("node shutdown")
        else:
            self.close_current_video()

            for target in self.targets:
                try:
                    if target["index_file"] is not None:
                        target["index_file"].flush()
                        target["index_file"].close()
                except Exception:
                    pass

        self.capture_running = False

        if self.capture_thread is not None:
            try:
                self.capture_thread.join(timeout=2.0)
            except Exception:
                pass

        if self.cap is not None:
            try:
                self.cap.release()
            except Exception:
                pass

        try:
            os.sync()
        except AttributeError:
            pass

        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)

    node = SeanoVideoLogger()

    try:
        rclpy.spin(node)

    except KeyboardInterrupt:
        pass

    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()