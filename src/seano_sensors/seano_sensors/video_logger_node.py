#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SEANO CA Debug Video Logger - Duration-Accurate Edition
=======================================================

Jaminan durasi video = durasi misi:
    Kode ini menggunakan pendekatan timestamp-based replay, bukan
    "tulis frame saat datang" yang bergantung pada kestabilan topic.

Cara kerja inti:
    1. Setiap frame yang datang dicap dengan monotonic timestamp ASLI.
    2. Frame + timestamp disimpan ke per-target FrameBuffer (list di RAM).
    3. Saat ARM -> DISARM, buffer di-flush ke VideoWriter dengan cara:
       - Hitung selisih waktu antar frame.
       - Jika ada gap lebih dari 1 frame interval, sisipkan frame duplikat
         sebanyak yang diperlukan supaya durasi video = durasi wall-clock.
    4. VideoWriter dibuka dengan output_fps tetap (default 30 fps untuk player
       compatibility), NAMUN jumlah frame yang ditulis disesuaikan agar
       durasi misi tersimpan dengan benar.

Manfaat pendekatan ini:
    - Durasi video di player = durasi misi sesungguhnya.
    - Tidak ada duplikat frame yang terbuang secara real-time.
    - Tahan terhadap jitter, drop, dan burst dari topic ROS.
    - Kompatibel dengan semua video player karena FPS di header MP4 adalah
      angka round (misal 30), bukan angka desimal aneh.
    - Dapat digunakan dengan topic yang FPS-nya tidak stabil sama sekali.

Catatan RAM:
    Buffer frame disimpan di RAM. Untuk misi panjang dengan resolusi besar,
    konsumsi RAM bisa tinggi. Gunakan `max_buffer_frames` untuk membatasi
    jumlah frame yang di-buffer jika diperlukan. Jika buffer penuh, frame
    lama yang pertama dibuang (FIFO circular).

Output:
    <MISSION_ROOT>/video/ca_debug_video_YYYY-MM-DD_HH-MM-SS_WIB.recording.mp4
    <MISSION_ROOT>/video/ca_debug_video_YYYY-MM-DD_HH-MM-SS_WIB.mp4

Author  : SEANO Engineering
Version : 3.0 (Duration-Accurate Edition)
"""

import os
import time
import math
from datetime import datetime
from collections import deque
from threading import Lock

import cv2
import numpy as np
from cv_bridge import CvBridge

import rclpy
from rclpy.node import Node
from rclpy.qos import (
    QoSProfile,
    ReliabilityPolicy,
    HistoryPolicy,
    DurabilityPolicy,
)

from sensor_msgs.msg import Image, CompressedImage
from mavros_msgs.msg import State


# =========================================================================
# Frame buffer: menyimpan frame + timestamp monotonic
# =========================================================================
class FrameBuffer:
    """
    Buffer frame yang timestamp-aware.

    Setiap entry adalah tuple (monotonic_timestamp_sec, numpy_array_BGR).
    Jika max_frames > 0, buffer bersifat circular (FIFO, frame lama dibuang).
    """

    def __init__(self, max_frames: int = 0):
        self._max = max_frames
        self._lock = Lock()
        self._frames: list = []  # list of (float, np.ndarray)

    def append(self, timestamp: float, frame: np.ndarray):
        with self._lock:
            self._frames.append((timestamp, frame))
            if self._max > 0 and len(self._frames) > self._max:
                self._frames.pop(0)

    def drain(self) -> list:
        """Kembalikan semua frame dan kosongkan buffer."""
        with self._lock:
            result = self._frames[:]
            self._frames.clear()
            return result

    def clear(self):
        with self._lock:
            self._frames.clear()

    def __len__(self):
        with self._lock:
            return len(self._frames)


# =========================================================================
# Timestamp-accurate video finalizer
# =========================================================================
def flush_buffer_to_video(
    frames: list,
    output_path: str,
    output_fps: float,
    fourcc_code: str,
    logger=None,
) -> bool:
    """
    Tulis buffer frame ke file video dengan durasi yang akurat.

    Algoritma:
        - Iterasi melalui frame yang sudah di-timestamp.
        - Untuk setiap pasang frame berurutan, hitung gap waktu nyata.
        - Konversi gap waktu ke jumlah frame berdasarkan output_fps.
        - Sisipkan frame duplikat (repeat frame terakhir) untuk mengisi gap.
        - Ini memastikan durasi video di player = durasi wall-clock.

    Param:
        frames      : list of (timestamp_sec, np.ndarray BGR)
        output_path : path file MP4 output
        output_fps  : FPS yang akan ditulis ke header MP4 (misal 30.0)
        fourcc_code : 4-char string misal "mp4v"
        logger      : rclpy logger atau None

    Return:
        True jika berhasil, False jika gagal.
    """
    if not frames:
        if logger:
            logger.warning("FLUSH | no frames to write")
        return False

    def log(msg):
        if logger:
            logger.info(msg)

    def log_warn(msg):
        if logger:
            logger.warning(msg)

    first_frame = frames[0][1]
    height, width = first_frame.shape[:2]
    frame_interval = 1.0 / output_fps

    fourcc = cv2.VideoWriter_fourcc(*fourcc_code)
    writer = cv2.VideoWriter(output_path, fourcc, float(output_fps), (int(width), int(height)))

    if not writer.isOpened():
        log_warn(f"FLUSH | VideoWriter failed to open: {output_path}")
        return False

    total_written = 0
    total_duplicated = 0

    try:
        for i, (ts, frame) in enumerate(frames):
            # Resize jika perlu (misal resolusi berubah di tengah misi)
            h, w = frame.shape[:2]
            if w != width or h != height:
                frame = cv2.resize(frame, (width, height), interpolation=cv2.INTER_AREA)

            # Tulis frame ini
            writer.write(frame)
            total_written += 1

            # Hitung berapa frame duplikat perlu disisipkan sebelum frame berikutnya
            if i + 1 < len(frames):
                next_ts = frames[i + 1][0]
                gap_sec = next_ts - ts

                if gap_sec > frame_interval * 1.5:
                    # Ada gap lebih dari 1,5x interval — sisipkan duplikat
                    extra_frames = max(0, round(gap_sec / frame_interval) - 1)
                    for _ in range(extra_frames):
                        writer.write(frame)
                        total_duplicated += 1
                        total_written += 1

    except Exception as exc:
        log_warn(f"FLUSH | error during write: {exc}")
        writer.release()
        return False

    writer.release()

    if not frames:
        return False

    real_duration = frames[-1][0] - frames[0][0]
    expected_video_sec = total_written / output_fps

    log(
        f"FLUSH DONE | file={os.path.basename(output_path)} | "
        f"raw_frames={len(frames)} | written={total_written} | "
        f"duplicated={total_duplicated} | "
        f"real_duration={real_duration:.2f}s | "
        f"video_duration={expected_video_sec:.2f}s | "
        f"output_fps={output_fps:.1f}"
    )

    return True


# =========================================================================
# Main node
# =========================================================================
class SeanoVideoLogger(Node):
    def __init__(self):
        super().__init__("video_logger_node")

        # ============================================================
        # PARAMETERS
        # ============================================================
        self.declare_parameter("image_topic", "/ca/debug_image")
        self.declare_parameter("image_type", "raw")          # raw / compressed
        self.declare_parameter("image_reliability", "best_effort")

        self.declare_parameter("external_mount_point", "/mnt/seano/SEANO_SSD")
        self.declare_parameter(
            "local_mount_point",
            os.path.expanduser("~/Documents/SEANO_logs"),
        )

        self.declare_parameter("enable_external_logging", True)
        self.declare_parameter("enable_local_logging", True)
        self.declare_parameter("auto_enable_external_if_ready", True)

        self.declare_parameter("require_external_on_mission", False)
        self.declare_parameter("require_local_on_mission", True)

        # FPS yang dipakai di header MP4 output.
        # Sebaiknya angka round yang kompatibel dengan player (15, 25, 30).
        # Durasi video tidak bergantung pada nilai ini — yang penting jumlah
        # frame yang ditulis sesuai perhitungan gap timestamp.
        self.declare_parameter("output_fps", 30.0)

        # Jika > 0, buffer frame dibatasi (circular FIFO). Hati-hati RAM.
        # 0 = tidak ada batas (tidak disarankan untuk misi sangat panjang).
        self.declare_parameter("max_buffer_frames", 0)

        self.declare_parameter("codec", "mp4v")
        self.declare_parameter("record_every_n_frames", 1)

        # Kompatibilitas YAML lama. Tidak dipakai.
        self.declare_parameter("segment_seconds", 0.0)

        self.declare_parameter("mission_gate_topic", "/mavros/state")
        self.declare_parameter("mission_folder_wait_sec", 10.0)
        self.declare_parameter("mission_folder_prefix", "MISSION_START_")
        self.declare_parameter("mission_folder_arm_grace_sec", 5.0)

        self.declare_parameter("required_image_max_age_s", 2.0)

        # Kompatibilitas lama.
        self.declare_parameter("force_record_without_mavros", False)

        # ============================================================
        # LOAD PARAMETERS
        # ============================================================
        self.image_topic = str(self.get_parameter("image_topic").value)
        self.image_type = str(self.get_parameter("image_type").value).strip().lower()
        self.image_reliability = str(
            self.get_parameter("image_reliability").value
        ).strip().lower()

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
        self.auto_enable_external_if_ready = bool(
            self.get_parameter("auto_enable_external_if_ready").value
        )
        self.require_external_on_mission = bool(
            self.get_parameter("require_external_on_mission").value
        )
        self.require_local_on_mission = bool(
            self.get_parameter("require_local_on_mission").value
        )

        self.output_fps = float(self.get_parameter("output_fps").value)
        if self.output_fps <= 0.0:
            self.output_fps = 30.0

        self.max_buffer_frames = max(0, int(self.get_parameter("max_buffer_frames").value))

        self.codec = str(self.get_parameter("codec").value)
        if len(self.codec) != 4:
            self.codec = "mp4v"

        self.record_every_n_frames = max(
            1, int(self.get_parameter("record_every_n_frames").value)
        )

        self.mission_gate_topic = str(self.get_parameter("mission_gate_topic").value)
        self.mission_folder_wait_sec = max(
            0.0, float(self.get_parameter("mission_folder_wait_sec").value)
        )
        self.mission_folder_prefix = str(
            self.get_parameter("mission_folder_prefix").value
        )
        self.mission_folder_arm_grace_sec = max(
            0.0, float(self.get_parameter("mission_folder_arm_grace_sec").value)
        )
        self.required_image_max_age_s = max(
            0.1, float(self.get_parameter("required_image_max_age_s").value)
        )

        if self.image_type not in ("raw", "compressed"):
            self.image_type = "raw"

        if (
            not self.enable_external_logging
            and self.auto_enable_external_if_ready
            and self.is_path_writable(self.external_mount_point)
        ):
            self.enable_external_logging = True

        # ============================================================
        # STATE
        # ============================================================
        self.bridge = CvBridge()

        self.state_received = False
        self.last_armed_state = False
        self.last_connected_state = False
        self.last_flight_mode = "UNKNOWN"

        self.current_arm_wall_epoch = None
        self.current_arm_local_time = None
        self.arm_monotonic = None         # time.monotonic() saat ARM
        self.disarm_monotonic = None      # time.monotonic() saat DISARM

        self.frame_seen = False
        self.last_frame_wall = 0.0

        self.logging_active = False
        self.preparing_session = False
        self.mission_id = None
        self.local_timezone = time.tzname[0]

        self.targets: list = []           # list of target dict
        self.video_filename: str = None
        self.temp_video_filename: str = None

        self.frame_count_in = 0           # total frame masuk dari topic
        self.frame_count_buffered = 0     # frame yang masuk ke buffer

        self.last_storage_warn_time = 0.0
        self.last_decode_warn_time = 0.0
        self.last_waiting_warn_time = 0.0

        # ============================================================
        # ROS INTERFACES
        # ============================================================
        self.create_subscription(
            State,
            self.mission_gate_topic,
            self.mavros_state_callback,
            self.make_state_qos(),
        )

        if self.image_type == "compressed":
            self.create_subscription(
                CompressedImage,
                self.image_topic,
                self.compressed_image_callback,
                self.make_image_qos(),
            )
        else:
            self.create_subscription(
                Image,
                self.image_topic,
                self.raw_image_callback,
                self.make_image_qos(),
            )

        self.get_logger().info(
            "VIDEO LOGGER STANDBY | "
            f"topic={self.image_topic} | type={self.image_type} | "
            f"mode=duration_accurate | output_fps={self.output_fps:.1f} | "
            f"max_buffer={self.max_buffer_frames or 'unlimited'} | "
            f"external={self.enable_external_logging} | local={self.enable_local_logging}"
        )

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

    def make_image_qos(self):
        reliability = (
            ReliabilityPolicy.RELIABLE
            if self.image_reliability == "reliable"
            else ReliabilityPolicy.BEST_EFFORT
        )
        return QoSProfile(
            history=HistoryPolicy.KEEP_LAST,
            depth=5,
            reliability=reliability,
            durability=DurabilityPolicy.VOLATILE,
        )

    # ================================================================
    # MAVROS STATE
    # ================================================================
    def mavros_state_callback(self, msg):
        previous_armed = self.last_armed_state

        self.state_received = True
        self.last_armed_state = bool(msg.armed)
        self.last_connected_state = bool(msg.connected)
        self.last_flight_mode = str(msg.mode)

        # Transisi DISARM -> ARM
        if msg.armed and not previous_armed:
            self.current_arm_wall_epoch = time.time()
            self.current_arm_local_time = datetime.now()
            self.arm_monotonic = time.monotonic()
            self.get_logger().info(
                f"ARMED | mode={msg.mode} | connected={msg.connected}"
            )

            if not self.logging_active and not self.preparing_session:
                if not self.is_image_ready():
                    self.log_waiting_image_once()

        # Transisi ARM -> DISARM
        if (not msg.armed) and previous_armed:
            self.disarm_monotonic = time.monotonic()
            self.get_logger().info(
                f"DISARMED | mode={msg.mode} | connected={msg.connected}"
            )

            if self.logging_active or self.preparing_session:
                self.stop_logging_session()
            else:
                self.abort_preparing_session()

            self.current_arm_wall_epoch = None
            self.current_arm_local_time = None
            self.arm_monotonic = None
            self.disarm_monotonic = None
            return

        if not msg.armed:
            self.current_arm_wall_epoch = None
            self.current_arm_local_time = None
            self.arm_monotonic = None

    # ================================================================
    # IMAGE CALLBACKS
    # ================================================================
    def compressed_image_callback(self, msg):
        self.frame_count_in += 1
        if self.frame_count_in % self.record_every_n_frames != 0:
            return

        np_arr = np.frombuffer(msg.data, np.uint8)
        frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

        if frame is None:
            self.warn_decode_once("failed decoding compressed image")
            return

        self.process_frame(frame)

    def raw_image_callback(self, msg):
        self.frame_count_in += 1
        if self.frame_count_in % self.record_every_n_frames != 0:
            return

        try:
            frame = self.bridge.imgmsg_to_cv2(msg, desired_encoding="bgr8")
        except Exception as exc:
            self.warn_decode_once(f"failed converting raw image: {exc}")
            return

        if frame is None:
            return

        self.process_frame(frame)

    def process_frame(self, frame):
        now_mono = time.monotonic()
        self.last_frame_wall = now_mono

        if not self.frame_seen:
            self.frame_seen = True
            self.get_logger().info("IMAGE TOPIC READY")

        if not self.last_armed_state:
            return

        # Mulai session jika belum
        if not self.logging_active and not self.preparing_session:
            if not self.start_session():
                return

        if self.logging_active:
            self.buffer_frame(now_mono, frame)

    def is_image_ready(self):
        if not self.frame_seen or self.last_frame_wall <= 0.0:
            return False
        return (time.monotonic() - self.last_frame_wall) <= self.required_image_max_age_s

    def log_waiting_image_once(self):
        now = time.monotonic()
        if now - self.last_waiting_warn_time < 10.0:
            return
        self.last_waiting_warn_time = now
        self.get_logger().info("VIDEO WAITING | armed but image topic not ready")

    def warn_decode_once(self, text):
        now = time.monotonic()
        if now - self.last_decode_warn_time < 10.0:
            return
        self.last_decode_warn_time = now
        self.get_logger().warning(f"IMAGE WARNING | {text}")

    # ================================================================
    # SESSION: START / STOP / ABORT
    # ================================================================
    def start_session(self):
        """
        Cari mission folder yang valid, siapkan target buffer.
        Dipanggil saat frame pertama setelah ARM tiba.
        """
        self.preparing_session = True

        if self.current_arm_wall_epoch is None:
            self.current_arm_wall_epoch = time.time()
            self.current_arm_local_time = datetime.now()
            self.arm_monotonic = time.monotonic()

        if not self.prepare_logging_targets_from_existing_mission():
            self.warn_storage_once(
                "VIDEO NOT STARTED | mission folder not found or storage not ready"
            )
            self.preparing_session = False
            return False

        self.video_filename = self.make_video_filename()
        self.temp_video_filename = self.video_filename.replace(".mp4", ".recording.mp4")

        self.frame_count_buffered = 0
        self.frame_count_in = 0

        self.logging_active = True
        self.preparing_session = False

        names = ", ".join(t["name"] for t in self.targets)
        self.get_logger().info(
            f"VIDEO RECORDING STARTED | mission_id={self.mission_id} | "
            f"output_fps={self.output_fps:.1f} | mode=duration_accurate | targets={names}"
        )
        return True

    def buffer_frame(self, timestamp: float, frame: np.ndarray):
        """Simpan frame ke semua target buffer."""
        if not self.logging_active:
            return

        frame_copy = frame.copy()

        for target in self.targets:
            buf: FrameBuffer = target.get("buffer")
            if buf is not None:
                buf.append(timestamp, frame_copy)

        self.frame_count_buffered += 1

    def stop_logging_session(self):
        """Dipanggil saat DISARM. Flush buffer ke file video."""
        if not self.logging_active and not self.preparing_session:
            return

        if self.preparing_session:
            self.abort_preparing_session()
            return

        self.logging_active = False
        self.preparing_session = False

        # Hitung durasi misi dari monotonic
        mission_duration = 0.0
        if self.arm_monotonic is not None and self.disarm_monotonic is not None:
            mission_duration = self.disarm_monotonic - self.arm_monotonic
        elif self.arm_monotonic is not None:
            mission_duration = time.monotonic() - self.arm_monotonic

        self.get_logger().info(
            f"VIDEO FINALIZING | buffered={self.frame_count_buffered} frames | "
            f"mission_duration={mission_duration:.2f}s"
        )

        self._flush_all_targets()

        try:
            os.sync()
        except Exception:
            pass

        self.targets = []
        self.video_filename = None
        self.temp_video_filename = None
        self.get_logger().info("VIDEO LOGGER STANDBY")

    def abort_preparing_session(self):
        self.preparing_session = False
        self.logging_active = False

        for target in self.targets:
            buf = target.get("buffer")
            if buf is not None:
                buf.clear()

        self.targets = []
        self.video_filename = None
        self.temp_video_filename = None
        self.get_logger().info("VIDEO SESSION ABORTED | VIDEO LOGGER STANDBY")

    # ================================================================
    # FLUSH BUFFER -> VIDEO FILE
    # ================================================================
    def _flush_all_targets(self):
        for target in self.targets:
            buf: FrameBuffer = target.get("buffer")
            if buf is None:
                continue

            frames = buf.drain()
            if not frames:
                self.get_logger().warning(
                    f"FLUSH | target={target['name']} | no frames buffered"
                )
                continue

            video_dir = target["video_dir"]
            temp_path = os.path.join(video_dir, self.temp_video_filename)
            final_path = os.path.join(video_dir, self.video_filename)

            self.get_logger().info(
                f"FLUSH START | target={target['name']} | frames={len(frames)} | "
                f"output={self.video_filename}"
            )

            success = flush_buffer_to_video(
                frames=frames,
                output_path=temp_path,
                output_fps=self.output_fps,
                fourcc_code=self.codec,
                logger=self.get_logger(),
            )

            if success:
                try:
                    os.replace(temp_path, final_path)
                    self.get_logger().info(
                        f"VIDEO SAVED | target={target['name']} | file={final_path}"
                    )
                except Exception as exc:
                    self.get_logger().error(
                        f"VIDEO RENAME ERROR | target={target['name']} | {exc}"
                    )
            else:
                self.get_logger().error(
                    f"VIDEO FLUSH FAILED | target={target['name']}"
                )

    # ================================================================
    # STORAGE / MISSION FOLDER ATTACH
    # ================================================================
    def prepare_logging_targets_from_existing_mission(self):
        deadline = time.time() + self.mission_folder_wait_sec
        while time.time() <= deadline:
            if self.try_attach_existing_mission_once():
                return True
            time.sleep(0.2)
        return False

    def try_attach_existing_mission_once(self):
        if self.current_arm_wall_epoch is None:
            self.current_arm_wall_epoch = time.time()

        min_mtime = self.current_arm_wall_epoch - self.mission_folder_arm_grace_sec
        candidates = []
        roots = self.get_today_roots()

        for target_name, day_root in roots:
            latest = self.find_latest_mission_folder(day_root, min_mtime=min_mtime)
            if latest is None:
                continue
            try:
                mtime = os.path.getmtime(latest)
            except OSError:
                mtime = 0.0
            candidates.append((mtime, os.path.basename(latest)))

        if not candidates:
            return False

        candidates.sort(key=lambda item: item[0], reverse=True)
        selected_mission_id = candidates[0][1]
        prepared = []

        for target_name, day_root in roots:
            if target_name == "external":
                if not self.is_path_writable(self.external_mount_point):
                    if self.require_external_on_mission:
                        return False
                    continue

            if target_name == "local":
                if not self.is_path_writable(self.local_mount_point):
                    if self.require_local_on_mission:
                        return False
                    continue

            mission_base_path = os.path.join(day_root, selected_mission_id)

            if not os.path.isdir(mission_base_path):
                if target_name == "external" and self.require_external_on_mission:
                    return False
                if target_name == "local" and self.require_local_on_mission:
                    return False
                continue

            try:
                folder_mtime = os.path.getmtime(mission_base_path)
            except OSError:
                folder_mtime = 0.0

            if folder_mtime < min_mtime:
                continue

            if not self.test_write_access(mission_base_path):
                if target_name == "external" and self.require_external_on_mission:
                    return False
                if target_name == "local" and self.require_local_on_mission:
                    return False
                continue

            video_dir = os.path.join(mission_base_path, "video")
            try:
                os.makedirs(video_dir, exist_ok=True)
            except Exception:
                continue

            prepared.append((target_name, video_dir))

        if not prepared:
            return False

        self.targets = []
        self.mission_id = selected_mission_id

        for target_name, video_dir in prepared:
            self.targets.append(
                {
                    "name": target_name,
                    "video_dir": video_dir,
                    "buffer": FrameBuffer(max_frames=self.max_buffer_frames),
                }
            )
            self.get_logger().info(
                f"VIDEO TARGET READY | target={target_name} | video_dir={video_dir} | "
                f"max_buffer={self.max_buffer_frames or 'unlimited'}"
            )

        return len(self.targets) > 0

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

        return roots

    def find_latest_mission_folder(self, day_root, min_mtime=None):
        if not os.path.isdir(day_root):
            return None

        candidates = []
        try:
            names = os.listdir(day_root)
        except Exception:
            return None

        for name in names:
            if not name.startswith(self.mission_folder_prefix):
                continue
            path = os.path.join(day_root, name)
            if not os.path.isdir(path):
                continue
            try:
                mtime = os.path.getmtime(path)
            except OSError:
                continue
            if min_mtime is not None and mtime < min_mtime:
                continue
            candidates.append((mtime, path))

        if not candidates:
            return None

        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    # ================================================================
    # FILENAME HELPER
    # ================================================================
    def make_video_filename(self):
        now = datetime.now()
        stamp = now.strftime(f"%Y-%m-%d_%H-%M-%S_{self.local_timezone}")
        return f"ca_debug_video_{stamp}.mp4"

    # ================================================================
    # UTILITIES
    # ================================================================
    def warn_storage_once(self, text):
        now = time.monotonic()
        if now - self.last_storage_warn_time < 10.0:
            return
        self.last_storage_warn_time = now
        self.get_logger().warning(text)

    @staticmethod
    def is_path_writable(path):
        return os.path.exists(path) and os.access(path, os.W_OK)

    @staticmethod
    def test_write_access(path):
        test_file = os.path.join(path, ".seano_video_write_test")
        try:
            os.makedirs(path, exist_ok=True)
            with open(test_file, "w") as f:
                f.write("ok")
            os.remove(test_file)
            return True
        except Exception:
            return False

    def destroy_node(self):
        if self.logging_active or self.preparing_session:
            self.get_logger().info("NODE SHUTDOWN | flushing video buffer...")
            self.disarm_monotonic = time.monotonic()
            self.stop_logging_session()
        else:
            for target in self.targets:
                buf = target.get("buffer")
                if buf:
                    buf.clear()

        try:
            os.sync()
        except Exception:
            pass

        super().destroy_node()


# =========================================================================
# ENTRY POINT
# =========================================================================
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