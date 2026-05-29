#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import glob
import math
import os
import socket
import time
import threading
import zipfile
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
from html import escape as xml_escape

import psutil
import rclpy
from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node
from rclpy.qos import qos_profile_sensor_data

from sensor_msgs.msg import BatteryState, Imu, NavSatFix
from std_msgs.msg import Float64MultiArray
from mavros_msgs.msg import (
    State,
    StatusText,
    VfrHud,
    WaypointList,
    WaypointReached,
)


@dataclass
class SensorSample:
    sensor: str
    recv_wall_ns: int
    recv_ros_ns: int
    msg_ros_ns: Optional[int]
    mission_elapsed_sec: float
    payload: Dict[str, Any]


def mean(values):
    return sum(values) / len(values) if values else math.nan


def median(values):
    if not values:
        return math.nan
    values = sorted(values)
    n = len(values)
    mid = n // 2
    if n % 2:
        return values[mid]
    return 0.5 * (values[mid - 1] + values[mid])


class SeanoLogger(Node):
    def __init__(self):
        super().__init__("logger_node")

        # ============================================================
        # STORAGE CONFIG
        # ============================================================
        self.external_mount_point = "/mnt/seano/SEANO_SSD"
        self.local_mount_point = os.path.expanduser("~/Documents/SEANO_logs")

        self.enable_external_logging = True
        self.enable_local_logging = True
        self.require_external_on_mission = False

        # ============================================================
        # SYNC CONFIG
        # ============================================================
        self.sensors = ["gps", "imu", "ctd", "adcp", "sbes", "battery"]

        self.sync_rate_hz = 1.0
        self.sync_period_ns = int(1e9 / self.sync_rate_hz)

        # Output delay hanya untuk menunggu data sensor terlambat masuk buffer.
        # Jika sensor reader sudah rapi, bisa diturunkan ke 250-400 ms.
        self.sync_output_delay_ms = 800.0
        self.sync_output_delay_ns = int(self.sync_output_delay_ms * 1e6)

        # Batas validasi data terhadap sync_time.
        self.sync_tolerance_ms = 250.0
        self.write_partial_frames = True

        self.sensor_flush_interval = 0.2
        self.metrics_interval = 1.0
        self.mission_timeline_interval = 1.0
        self.internet_check_interval = 1.0

        # ============================================================
        # INTERNET MONITOR CONFIG
        # ============================================================
        self.internet_probe_host = "1.1.1.1"
        self.internet_probe_port = 53
        self.internet_probe_timeout = 0.4

        self.cloud_probe_host = "mqtt.seano.cloud"
        self.cloud_probe_port = 8883
        self.cloud_probe_timeout = 0.4

        # ============================================================
        # RUNTIME STATE
        # ============================================================
        self.cb_group = ReentrantCallbackGroup()
        self.lock = threading.RLock()

        self.logging_active = False
        self.start_time_obj = None
        self.end_time_obj = None
        self.mission_start_monotonic = None
        self.local_timezone = time.tzname[0]
        self.mission_id = None
        self.base_paths: List[str] = []

        self.external_ready = False
        self.external_failed_runtime = False
        self.external_fail_reported = False

        # MAVROS / mission cache.
        self.last_connected_state = False
        self.last_armed_state = False
        self.last_guided_state = False
        self.last_manual_input_state = False
        self.last_flight_mode = "UNKNOWN"
        self.last_flight_mode_class = "OTHER"
        self.last_system_status = 0
        self.prev_flight_mode = "UNKNOWN"
        self.prev_flight_mode_class = "OTHER"

        self.current_waypoint_seq = -1
        self.last_reached_waypoint_seq = -1
        self.last_waypoint_count = 0
        self.last_waypoints_msg = None
        self.last_waypoints_signature = None

        self.last_groundspeed = math.nan
        self.last_airspeed = math.nan
        self.last_heading = math.nan
        self.last_throttle = math.nan
        self.last_vfr_altitude = math.nan
        self.last_climb = math.nan

        self.last_latitude = math.nan
        self.last_longitude = math.nan
        self.last_gps_altitude = math.nan
        self.last_gps_status = math.nan
        self.last_gps_service = math.nan

        self.last_battery_voltage = math.nan
        self.last_battery_current = math.nan
        self.last_battery_percent = math.nan
        self.mission_start_battery_percent = math.nan
        self.mission_end_battery_percent = math.nan

        # Mission statistics.
        self.mission_event_count = 0
        self.mission_return_home_detected = False
        self.mission_failsafe_detected = False
        self.mission_max_speed_mps = 0.0
        self.mission_speed_sum_mps = 0.0
        self.mission_speed_sample_count = 0
        self.mission_distance_m = 0.0
        self.mission_last_distance_lat = None
        self.mission_last_distance_lon = None
        self.last_mission_timeline_slot = -1
        self.last_mission_timeline_write_time = 0.0

        # Internet state.
        self.internet_online = None
        self.cloud_online = None
        self.internet_lost_start_time = None
        self.cloud_lost_start_time = None
        self.internet_lost_count = 0
        self.cloud_lost_count = 0
        self.internet_total_down_sec = 0.0
        self.cloud_total_down_sec = 0.0
        self.last_internet_write_slot = -1

        # Sensor buffers.
        self.buffers: Dict[str, deque] = {key: deque(maxlen=500) for key in self.sensors}
        self.sensor_write_queues: Dict[str, deque] = {key: deque(maxlen=5000) for key in self.sensors}
        self.sample_count: Dict[str, int] = {key: 0 for key in self.sensors}
        self.last_sensor_rx_wall_ns: Dict[str, int] = {key: 0 for key in self.sensors}

        # Sensor CSV terpisah ditulis 1 Hz per sensor, seperti logger lama.
        # Buffer tetap menyimpan semua sample yang masuk agar synchronized_log bisa memilih sample terdekat.
        self.last_sensor_csv_slot: Dict[str, int] = {key: -1 for key in self.sensors}

        # Sync statistics.
        self.last_sync_target_ns = 0
        self.sync_frame_count = 0
        self.sync_valid_frame_count = 0
        self.sync_partial_frame_count = 0
        self.sync_missing_frame_count = 0
        self.sync_delta_stats_ms: Dict[str, List[float]] = {key: [] for key in self.sensors}
        self.sync_age_stats_ms: Dict[str, List[float]] = {key: [] for key in self.sensors}
        self.sync_status_count: Dict[str, Dict[str, int]] = {
            key: {"valid": 0, "stale": 0, "missing": 0} for key in self.sensors
        }
        self.frame_span_stats_ms: List[float] = []

        # File handles.
        self.reset_file_handles()

        # Metrics.
        self.bytes_written_since_last_metrics = 0
        self.last_metrics_time = time.time()
        self.jetson_temp_source = None

        # ============================================================
        # SUBSCRIBERS
        # ============================================================
        self.create_subscription(State, "/mavros/state", self.mavros_state_callback, 10, callback_group=self.cb_group)
        self.create_subscription(WaypointList, "/mavros/mission/waypoints", self.waypoints_callback, qos_profile_sensor_data, callback_group=self.cb_group)
        self.create_subscription(WaypointReached, "/mavros/mission/reached", self.waypoint_reached_callback, qos_profile_sensor_data, callback_group=self.cb_group)
        self.create_subscription(VfrHud, "/mavros/vfr_hud", self.vfr_hud_callback, qos_profile_sensor_data, callback_group=self.cb_group)
        self.create_subscription(StatusText, "/mavros/statustext/recv", self.statustext_callback, qos_profile_sensor_data, callback_group=self.cb_group)

        self.create_subscription(NavSatFix, "/mavros/global_position/raw/fix", self.gps_callback, qos_profile_sensor_data, callback_group=self.cb_group)
        self.create_subscription(Imu, "/mavros/imu/data", self.imu_callback, qos_profile_sensor_data, callback_group=self.cb_group)
        self.create_subscription(Float64MultiArray, "/ctd/data", self.ctd_callback, 50, callback_group=self.cb_group)
        self.create_subscription(Float64MultiArray, "/adcp/data", self.adcp_callback, 10, callback_group=self.cb_group)
        self.create_subscription(Float64MultiArray, "/sbes/data", self.sbes_callback, 10, callback_group=self.cb_group)
        self.create_subscription(BatteryState, "/battery/state", self.battery_callback, qos_profile_sensor_data, callback_group=self.cb_group)

        # ============================================================
        # TIMERS
        # ============================================================
        self.create_timer(1.0 / self.sync_rate_hz, self.write_synchronized_frame, callback_group=self.cb_group)
        self.create_timer(self.sensor_flush_interval, self.flush_sensor_queues, callback_group=self.cb_group)
        self.create_timer(self.metrics_interval, self.log_periodic_metrics, callback_group=self.cb_group)
        self.create_timer(self.mission_timeline_interval, self.log_mission_timeline, callback_group=self.cb_group)
        self.create_timer(self.internet_check_interval, self.monitor_internet_simple, callback_group=self.cb_group)
        self.create_timer(1.0, self.monitor_external_storage, callback_group=self.cb_group)

        psutil.cpu_percent(interval=None)

        self.get_logger().info("SEANO Logger standby")
        self.get_logger().info("Mode: software-synchronized multi-sensor logger")
        self.get_logger().info("Mission gate aktif: logging hanya saat /mavros/state armed=True")
        self.get_logger().info(
            f"Sync rate={self.sync_rate_hz:.2f} Hz | "
            f"output_delay={self.sync_output_delay_ms:.0f} ms | "
            f"tolerance={self.sync_tolerance_ms:.0f} ms"
        )

    # ============================================================
    # TIME HELPERS
    # ============================================================
    def wall_ns(self):
        return time.time_ns()

    def ros_ns(self):
        return self.get_clock().now().nanoseconds

    def ns_to_local_timestamp(self, ns):
        return datetime.fromtimestamp(ns / 1e9).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def get_local_timestamp(self):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def get_unix_time(self):
        return time.time()

    def get_mission_elapsed_sec(self):
        if self.mission_start_monotonic is None:
            return 0.0
        return time.monotonic() - self.mission_start_monotonic

    def ros_stamp_to_float(self, stamp):
        return float(stamp.sec) + float(stamp.nanosec) * 1e-9

    def header_stamp_to_ns(self, msg):
        try:
            stamp = msg.header.stamp
            ns = int(stamp.sec) * 1_000_000_000 + int(stamp.nanosec)
            return ns if ns > 0 else None
        except Exception:
            return None

    def safe_value(self, value):
        if value is None:
            return ""
        if isinstance(value, float) and math.isnan(value):
            return ""
        return value

    # ============================================================
    # MODE / MISSION GATE
    # ============================================================
    def classify_mode(self, mode):
        m = str(mode).upper()
        if "RTL" in m or "RTH" in m or "RETURN" in m:
            return "RETURN_HOME"
        if "AUTO" in m and "RTL" not in m:
            return "AUTO_MISSION"
        if "GUIDED" in m:
            return "GUIDED"
        if "MANUAL" in m:
            return "MANUAL"
        if "LOITER" in m or "HOLD" in m:
            return "HOLD"
        if "STABILIZE" in m:
            return "STABILIZE"
        return "OTHER"

    def mavros_state_callback(self, msg):
        mode_class = self.classify_mode(msg.mode)

        self.last_connected_state = msg.connected
        self.last_armed_state = msg.armed
        self.last_guided_state = msg.guided
        self.last_manual_input_state = msg.manual_input
        self.last_flight_mode = msg.mode
        self.last_flight_mode_class = mode_class
        self.last_system_status = msg.system_status

        if msg.armed and not self.logging_active:
            self.start_logging_session(msg)

        elif not msg.armed and self.logging_active:
            self.write_mission_event("DISARM", f"mode={msg.mode}, armed={msg.armed}, connected={msg.connected}")
            self.stop_logging_session(f"mode={msg.mode}, armed={msg.armed}, connected={msg.connected}")

        if self.logging_active and msg.mode != self.prev_flight_mode:
            self.write_mission_state_change(
                self.prev_flight_mode,
                msg.mode,
                self.prev_flight_mode_class,
                mode_class,
            )
            self.write_mission_event("MODE_CHANGE", f"{self.prev_flight_mode} -> {msg.mode}")

            if mode_class == "RETURN_HOME":
                self.write_mission_event("RETURN_HOME_DETECTED", f"mode={msg.mode}")

        self.prev_flight_mode = msg.mode
        self.prev_flight_mode_class = mode_class

    def start_logging_session(self, state_msg=None):
        if self.logging_active:
            return

        self.get_logger().info("ARMED detected -> preparing synchronous logger session")
        self.reset_session_state()

        self.start_time_obj = datetime.now()
        self.end_time_obj = None
        self.local_timezone = time.tzname[0]
        self.mission_start_monotonic = time.monotonic()

        year = self.start_time_obj.strftime("%Y")
        month = self.start_time_obj.strftime("%m")
        day = self.start_time_obj.strftime("%d")
        self.mission_id = self.start_time_obj.strftime(f"MISSION_START_%H-%M-%S_{self.local_timezone}")

        if not self.prepare_base_paths(year, month, day):
            self.get_logger().fatal("Tidak ada path logging valid. Logger kembali standby.")
            return

        self.init_folder_structure()
        self.write_mission_start_info(state_msg)
        self.init_system_metrics_logger()
        self.init_sensor_loggers()
        self.init_mission_logger()

        self.logging_active = True
        self.last_metrics_time = time.time()
        self.bytes_written_since_last_metrics = 0
        psutil.cpu_percent(interval=None)

        if state_msg is not None:
            self.prev_flight_mode = state_msg.mode
            self.prev_flight_mode_class = self.classify_mode(state_msg.mode)

        self.write_mission_event("ARM", f"mode={self.last_flight_mode}, connected={self.last_connected_state}")

        if self.last_waypoints_msg is not None:
            self.dump_waypoints(self.last_waypoints_msg)

        for path in self.base_paths:
            self.get_logger().info(f"Mission folder: {path}")

        self.get_logger().info("SYNCHRONOUS LOGGER ACTIVE")

    def stop_logging_session(self, reason="vehicle disarmed"):
        if not self.logging_active:
            return

        self.logging_active = False
        self.end_time_obj = datetime.now()
        self.get_logger().info(f"DISARM detected -> closing logger session | {reason}")

        self.flush_sensor_queues(force=True)
        self.write_sync_quality_summary()
        self.write_mission_end_info(reason)
        self.close_all_files()
        self.export_synchronized_workbooks()

        try:
            os.sync()
        except AttributeError:
            pass

        self.reset_after_close()
        self.get_logger().info("SEANO Logger kembali standby, menunggu armed berikutnya")

    def reset_session_state(self):
        self.external_ready = False
        self.external_failed_runtime = False
        self.external_fail_reported = False
        self.base_paths = []
        self.mission_info_paths = []
        self.summary_paths = []
        self.sync_quality_paths = []

        with self.lock:
            for key in self.sensors:
                self.buffers[key].clear()
                self.sensor_write_queues[key].clear()
                self.sample_count[key] = 0
                self.last_sensor_rx_wall_ns[key] = 0
                self.last_sensor_csv_slot[key] = -1
                self.sync_delta_stats_ms[key] = []
                self.sync_age_stats_ms[key] = []
                self.sync_status_count[key] = {"valid": 0, "stale": 0, "missing": 0}

            self.frame_span_stats_ms = []
            self.last_sync_target_ns = 0
            self.sync_frame_count = 0
            self.sync_valid_frame_count = 0
            self.sync_partial_frame_count = 0
            self.sync_missing_frame_count = 0

        self.reset_file_handles()
        self.reset_user_friendly_mission_stats()

    def reset_after_close(self):
        self.base_paths = []
        self.external_ready = False
        self.external_failed_runtime = False
        self.external_fail_reported = False
        self.mission_start_monotonic = None
        self.reset_file_handles()

    def reset_file_handles(self):
        self.sensor_files = {key: [] for key in self.sensors}
        self.sensor_writers = {key: [] for key in self.sensors}
        self.sync_files = []
        self.sync_writers = []
        self.sync_diagnostic_files = []
        self.sync_diagnostic_writers = []
        self.system_metrics_files = []
        self.system_metrics_writers = []
        self.mission_dirs = []
        self.video_dirs = []

        self.mission_log_files = []
        self.mission_timeline_files = []
        self.mission_timeline_writers = []
        self.mission_readable_files = []
        self.mission_readable_writers = []
        self.mission_events_files = []
        self.mission_events_writers = []
        self.mission_state_files = []
        self.mission_state_writers = []
        self.mission_waypoint_reached_files = []
        self.mission_waypoint_reached_writers = []
        self.mission_waypoints_files = []
        self.mission_waypoints_writers = []
        self.mission_statustext_files = []
        self.mission_statustext_writers = []
        self.internet_status_files = []
        self.internet_status_writers = []
        self.internet_event_files = []

    # ============================================================
    # STORAGE
    # ============================================================
    def is_path_writable(self, path):
        return os.path.exists(path) and os.access(path, os.W_OK)

    def test_write_access(self, path):
        test_file = os.path.join(path, ".seano_write_test")
        try:
            os.makedirs(path, exist_ok=True)
            with open(test_file, "w") as f:
                f.write("ok")
            os.remove(test_file)
            return True
        except Exception:
            return False

    def prepare_base_paths(self, year, month, day):
        self.base_paths = []

        if self.enable_external_logging:
            if self.is_path_writable(self.external_mount_point):
                external_base_path = os.path.join(
                    self.external_mount_point,
                    "SEANO_MISSIONS",
                    year,
                    month,
                    day,
                    self.mission_id,
                )

                try:
                    os.makedirs(external_base_path, exist_ok=True)
                    if not self.test_write_access(external_base_path):
                        raise RuntimeError("External SSD detected but not writable")
                    self.base_paths.append(external_base_path)
                    self.external_ready = True
                    self.get_logger().info(f"External logging ready: {external_base_path}")
                except Exception as e:
                    self.external_ready = False
                    self.external_failed_runtime = True
                    self.get_logger().error(f"Gagal menyiapkan external logging: {external_base_path} | {e}")
            else:
                self.get_logger().warning(f"SSD external belum siap / tidak writable: {self.external_mount_point}")

            if self.require_external_on_mission and not self.external_ready:
                self.get_logger().fatal("Armed terdeteksi, tetapi SSD external belum siap. Logger tetap standby.")
                return False

        if self.enable_local_logging:
            local_base_path = os.path.join(self.local_mount_point, year, month, day, self.mission_id)

            try:
                os.makedirs(local_base_path, exist_ok=True)
                if not self.test_write_access(local_base_path):
                    raise RuntimeError("Local path not writable")
                self.base_paths.append(local_base_path)
                self.get_logger().info(f"Local logging ready: {local_base_path}")
            except Exception as e:
                self.get_logger().error(f"Gagal membuat folder local logging: {local_base_path} | {e}")

        return len(self.base_paths) > 0

    def init_folder_structure(self):
        for base_path in self.base_paths:
            mission_dir = os.path.join(base_path, "mission")
            sensor_dir = os.path.join(base_path, "sensor")
            video_dir = os.path.join(base_path, "video")

            os.makedirs(mission_dir, exist_ok=True)
            os.makedirs(sensor_dir, exist_ok=True)
            os.makedirs(video_dir, exist_ok=True)

            self.mission_dirs.append(mission_dir)
            self.video_dirs.append(video_dir)

    def monitor_external_storage(self):
        if not self.logging_active:
            return

        if not self.enable_external_logging or not self.external_ready:
            return

        if not self.is_path_writable(self.external_mount_point):
            if not self.external_fail_reported:
                self.get_logger().fatal(f"SSD external terputus / tidak writable lagi: {self.external_mount_point}")
                self.external_fail_reported = True
                self.external_failed_runtime = True

    # ============================================================
    # ROOT FILES
    # ============================================================
    def write_mission_start_info(self, state_msg=None):
        for base_path in self.base_paths:
            mission_info_path = os.path.join(base_path, "mission_info.txt")
            self.mission_info_paths.append(mission_info_path)

            with open(mission_info_path, "w") as f:
                f.write("SEANO Mission Info\n")
                f.write("==================\n")
                f.write(f"Start Time: {self.start_time_obj}\n")
                f.write(f"Timezone: {self.local_timezone}\n")
                f.write("Platform: SEANO USV\n")
                f.write("Logger Mode: Armed gated\n")
                f.write("Logger Type: Software-synchronized multi-sensor logger\n")
                f.write(f"Sync Rate Hz: {self.sync_rate_hz:.3f}\n")
                f.write(f"Sync Output Delay ms: {self.sync_output_delay_ms:.1f}\n")
                f.write(f"Sync Tolerance ms: {self.sync_tolerance_ms:.1f}\n")
                f.write("Sensor Files: CSV only\n")
                f.write("Folder Structure: root + mission/ + sensor/ + video/\n")
                f.write("Mission Gate: /mavros/state armed=True\n")
                if state_msg is not None:
                    f.write(f"Start MAVROS Connected: {state_msg.connected}\n")
                    f.write(f"Start MAVROS Armed: {state_msg.armed}\n")
                    f.write(f"Start MAVROS Mode: {state_msg.mode}\n")

    def write_mission_end_info(self, reason):
        end_time_obj = self.end_time_obj or datetime.now()
        elapsed = self.get_mission_elapsed_sec()
        self.write_mission_summary(reason, end_time_obj, elapsed)

        for mission_info_path in self.mission_info_paths:
            try:
                with open(mission_info_path, "a") as f:
                    f.write(f"End Time: {end_time_obj}\n")
                    f.write(f"Mission Duration Sec: {elapsed:.3f}\n")
                    f.write(f"Stop Reason: {reason}\n")
                    f.write(f"End MAVROS Connected: {self.last_connected_state}\n")
                    f.write(f"End MAVROS Armed: {self.last_armed_state}\n")
                    f.write(f"End MAVROS Mode: {self.last_flight_mode}\n")
                    f.write(f"Internet Final Status: {self.internet_text(self.internet_online)}\n")
                    f.write(f"MQTT Cloud Final Status: {self.internet_text(self.cloud_online)}\n")
            except Exception as e:
                self.get_logger().error(f"Failed writing mission end info: {e}")

    # ============================================================
    # SENSOR FILES
    # ============================================================
    def init_sensor_loggers(self):
        for base_path in self.base_paths:
            sensor_dir = os.path.join(base_path, "sensor")
            os.makedirs(sensor_dir, exist_ok=True)

            # File ini user-friendly: hanya data sensor hasil matching sinkron.
            # Delay/status/frame quality dipindah ke sync_diagnostics.csv.
            sync_path = os.path.join(sensor_dir, "synchronized_log.csv")
            sync_file = open(sync_path, "w", newline="", buffering=1)
            sync_writer = csv.writer(sync_file)
            sync_writer.writerow(self.sync_data_header())
            self.sync_files.append(sync_file)
            self.sync_writers.append(sync_writer)

            diagnostic_path = os.path.join(sensor_dir, "sync_diagnostics.csv")
            diagnostic_file = open(diagnostic_path, "w", newline="", buffering=1)
            diagnostic_writer = csv.writer(diagnostic_file)
            diagnostic_writer.writerow(self.sync_diagnostic_header())
            self.sync_diagnostic_files.append(diagnostic_file)
            self.sync_diagnostic_writers.append(diagnostic_writer)

            self.sync_quality_paths.append(os.path.join(sensor_dir, "sync_quality_summary.csv"))

            for sensor in self.sensors:
                sensor_path = os.path.join(sensor_dir, f"{sensor}.csv")
                sensor_file = open(sensor_path, "w", newline="", buffering=1)
                sensor_writer = csv.writer(sensor_file)
                sensor_writer.writerow(self.sensor_header(sensor))
                self.sensor_files[sensor].append(sensor_file)
                self.sensor_writers[sensor].append(sensor_writer)

    def sensor_header(self, sensor):
        base = ["timestamp", "mission_elapsed_sec"]

        if sensor == "gps":
            return base + ["latitude", "longitude", "altitude", "status", "service"]
        if sensor == "imu":
            return base + ["acc_x", "acc_y", "acc_z"]
        if sensor == "ctd":
            return base + ["depth", "temp", "cond", "salinity", "density", "soundvel"]
        if sensor == "adcp":
            return base + [
                "num_cells",
                "num_beams",
                "cell_size_m",
                "blanking_distance_m",
                "heading_deg",
                "pitch_deg",
                "roll_deg",
                "temperature_c",
                "salinity_psu",
                "pressure_dbar",
                "velocity_profile",
            ]
        if sensor == "sbes":
            return base + ["depth", "water_temp", "quality_flag"]
        if sensor == "battery":
            return base + ["voltage_v", "current_a", "percentage_percent"]

        return base + ["payload"]

    def payload_values(self, sensor, payload):
        if payload is None:
            return [""] * (len(self.sensor_header(sensor)) - 2)

        if sensor == "gps":
            return [
                self.safe_value(payload.get("latitude")),
                self.safe_value(payload.get("longitude")),
                self.safe_value(payload.get("altitude")),
                self.safe_value(payload.get("status")),
                self.safe_value(payload.get("service")),
            ]
        if sensor == "imu":
            return [self.safe_value(payload.get("acc_x")), self.safe_value(payload.get("acc_y")), self.safe_value(payload.get("acc_z"))]
        if sensor == "ctd":
            return [
                self.safe_value(payload.get("depth")),
                self.safe_value(payload.get("temp")),
                self.safe_value(payload.get("cond")),
                self.safe_value(payload.get("salinity")),
                self.safe_value(payload.get("density")),
                self.safe_value(payload.get("soundvel")),
            ]
        if sensor == "adcp":
            return [
                self.safe_value(payload.get("num_cells")),
                self.safe_value(payload.get("num_beams")),
                self.safe_value(payload.get("cell_size_m")),
                self.safe_value(payload.get("blanking_distance_m")),
                self.safe_value(payload.get("heading_deg")),
                self.safe_value(payload.get("pitch_deg")),
                self.safe_value(payload.get("roll_deg")),
                self.safe_value(payload.get("temperature_c")),
                self.safe_value(payload.get("salinity_psu")),
                self.safe_value(payload.get("pressure_dbar")),
                self.safe_value(payload.get("velocity_profile")),
            ]
        if sensor == "sbes":
            return [self.safe_value(payload.get("depth")), self.safe_value(payload.get("water_temp")), self.safe_value(payload.get("quality_flag"))]
        if sensor == "battery":
            return [self.safe_value(payload.get("voltage_v")), self.safe_value(payload.get("current_a")), self.safe_value(payload.get("percentage_percent"))]

        return [str(payload)]

    def push_sensor_sample(self, sensor, payload, msg_ros_ns=None):
        if not self.logging_active:
            return

        recv_wall_ns = self.wall_ns()
        recv_ros_ns = self.ros_ns()
        elapsed = self.get_mission_elapsed_sec()

        sample = SensorSample(
            sensor=sensor,
            recv_wall_ns=recv_wall_ns,
            recv_ros_ns=recv_ros_ns,
            msg_ros_ns=msg_ros_ns,
            mission_elapsed_sec=elapsed,
            payload=payload,
        )

        with self.lock:
            self.buffers[sensor].append(sample)
            self.last_sensor_rx_wall_ns[sensor] = recv_wall_ns
            self.sample_count[sensor] = self.sample_count.get(sensor, 0) + 1

            # File sensor individual dibuat 1 Hz per sensor supaya tetap mudah dibaca
            # dan tidak terlalu besar. Synchronized buffer tetap menyimpan semua sample.
            current_slot = int(recv_wall_ns // 1_000_000_000)
            if current_slot != self.last_sensor_csv_slot.get(sensor, -1):
                self.last_sensor_csv_slot[sensor] = current_slot

                row = [
                    self.ns_to_local_timestamp(recv_wall_ns),
                    f"{elapsed:.3f}",
                ] + self.payload_values(sensor, payload)

                self.sensor_write_queues[sensor].append(row)

    def flush_sensor_queues(self, force=False):
        if not self.logging_active and not force:
            return

        with self.lock:
            rows_by_sensor = {}
            for sensor in self.sensors:
                rows_by_sensor[sensor] = list(self.sensor_write_queues[sensor])
                self.sensor_write_queues[sensor].clear()

        for sensor, rows in rows_by_sensor.items():
            if not rows:
                continue

            for writer in self.sensor_writers.get(sensor, []):
                try:
                    writer.writerows(rows)
                except Exception as e:
                    self.get_logger().error(f"{sensor.upper()} CSV flush failed: {e}")

            self.bytes_written_since_last_metrics += sum(len(",".join(map(str, row)).encode("utf-8")) for row in rows)

    # ============================================================
    # SYNCHRONIZED FRAME
    # ============================================================
    def sync_data_header(self):
        # User-friendly synchronized data:
        # hanya waktu frame + data sensor hasil matching terdekat.
        header = ["sync_time", "mission_elapsed_sec"]

        for sensor in self.sensors:
            for column in self.payload_column_names(sensor):
                header.append(f"{sensor}_{column}")

        return header

    def sync_diagnostic_header(self):
        # Data teknis sinkronisasi dipisah agar synchronized_log.csv bersih.
        header = [
            "sync_time",
            "mission_elapsed_sec",
            "frame_status",
            "frame_span_ms",
            "valid_sensor_count",
        ]

        for sensor in self.sensors:
            header += [
                f"{sensor}_time",
                f"{sensor}_delay_ms",
                f"{sensor}_age_ms",
                f"{sensor}_status",
            ]

        return header

    def payload_column_names(self, sensor):
        return [name for name in self.sensor_header(sensor)[2:]]

    def get_nearest_sample(self, sensor, target_ns):
        with self.lock:
            data = list(self.buffers[sensor])

        if not data:
            return None

        return min(data, key=lambda sample: abs(sample.recv_wall_ns - target_ns))

    def write_synchronized_frame(self):
        if not self.logging_active or not self.sync_writers:
            return

        now_ns = self.wall_ns()
        target_ns = ((now_ns - self.sync_output_delay_ns) // self.sync_period_ns) * self.sync_period_ns

        if target_ns <= self.last_sync_target_ns:
            return

        self.last_sync_target_ns = target_ns

        samples = {}
        deltas_ms = {}
        ages_ms = {}
        statuses = {}
        sample_times = []
        valid_count = 0

        for sensor in self.sensors:
            sample = self.get_nearest_sample(sensor, target_ns)
            samples[sensor] = sample

            if sample is None:
                deltas_ms[sensor] = math.nan
                ages_ms[sensor] = math.nan
                statuses[sensor] = "missing"
                self.sync_status_count[sensor]["missing"] += 1
                continue

            delta_ms = (sample.recv_wall_ns - target_ns) / 1e6
            age_ms = (now_ns - sample.recv_wall_ns) / 1e6

            deltas_ms[sensor] = delta_ms
            ages_ms[sensor] = age_ms

            self.sync_delta_stats_ms[sensor].append(abs(delta_ms))
            self.sync_age_stats_ms[sensor].append(age_ms)

            if abs(delta_ms) <= self.sync_tolerance_ms:
                statuses[sensor] = "valid"
                self.sync_status_count[sensor]["valid"] += 1
                valid_count += 1
            else:
                statuses[sensor] = "stale"
                self.sync_status_count[sensor]["stale"] += 1

            sample_times.append(sample.recv_wall_ns)

        if sample_times:
            frame_span_ms = (max(sample_times) - min(sample_times)) / 1e6
            self.frame_span_stats_ms.append(frame_span_ms)
        else:
            frame_span_ms = math.nan

        if valid_count == len(self.sensors):
            frame_status = "valid"
            self.sync_valid_frame_count += 1
        elif valid_count > 0:
            frame_status = "partial"
            self.sync_partial_frame_count += 1
        else:
            frame_status = "missing"
            self.sync_missing_frame_count += 1

        self.sync_frame_count += 1

        if frame_status != "valid" and not self.write_partial_frames:
            return

        sync_time_str = self.ns_to_local_timestamp(target_ns)
        elapsed_str = f"{self.get_mission_elapsed_sec():.3f}"

        data_row = [sync_time_str, elapsed_str]

        for sensor in self.sensors:
            sample = samples[sensor]
            if sample is None:
                data_row += self.payload_values(sensor, None)
            else:
                data_row += self.payload_values(sensor, sample.payload)

        diagnostic_row = [
            sync_time_str,
            elapsed_str,
            frame_status,
            "" if math.isnan(frame_span_ms) else f"{frame_span_ms:.3f}",
            valid_count,
        ]

        for sensor in self.sensors:
            sample = samples[sensor]

            if sample is None:
                diagnostic_row += ["", "", "", "missing"]
                continue

            diagnostic_row += [
                self.ns_to_local_timestamp(sample.recv_wall_ns),
                f"{deltas_ms[sensor]:.3f}",
                f"{ages_ms[sensor]:.3f}",
                statuses[sensor],
            ]

        for writer in self.sync_writers:
            try:
                writer.writerow(data_row)
            except Exception as e:
                self.get_logger().error(f"Synchronized data write failed: {e}")

        for writer in self.sync_diagnostic_writers:
            try:
                writer.writerow(diagnostic_row)
            except Exception as e:
                self.get_logger().error(f"Synchronized diagnostics write failed: {e}")

        self.bytes_written_since_last_metrics += len(",".join(map(str, data_row)).encode("utf-8"))
        self.bytes_written_since_last_metrics += len(",".join(map(str, diagnostic_row)).encode("utf-8"))

    def write_sync_quality_summary(self):
        for path in self.sync_quality_paths:
            try:
                with open(path, "w", newline="") as f:
                    writer = csv.writer(f)

                    writer.writerow(["summary_key", "value"])
                    writer.writerow(["sync_frame_count", self.sync_frame_count])
                    writer.writerow(["valid_frame_count", self.sync_valid_frame_count])
                    writer.writerow(["partial_frame_count", self.sync_partial_frame_count])
                    writer.writerow(["missing_frame_count", self.sync_missing_frame_count])
                    writer.writerow(["sync_rate_hz", self.sync_rate_hz])
                    writer.writerow(["sync_output_delay_ms", self.sync_output_delay_ms])
                    writer.writerow(["sync_tolerance_ms", self.sync_tolerance_ms])

                    if self.frame_span_stats_ms:
                        writer.writerow(["frame_span_mean_ms", f"{mean(self.frame_span_stats_ms):.3f}"])
                        writer.writerow(["frame_span_median_ms", f"{median(self.frame_span_stats_ms):.3f}"])
                        writer.writerow(["frame_span_max_ms", f"{max(self.frame_span_stats_ms):.3f}"])
                    else:
                        writer.writerow(["frame_span_mean_ms", ""])
                        writer.writerow(["frame_span_median_ms", ""])
                        writer.writerow(["frame_span_max_ms", ""])

                    writer.writerow([])
                    writer.writerow([
                        "sensor",
                        "samples_received",
                        "valid_count",
                        "stale_count",
                        "missing_count",
                        "delta_abs_mean_ms",
                        "delta_abs_median_ms",
                        "delta_abs_max_ms",
                        "age_mean_ms",
                        "age_median_ms",
                        "age_max_ms",
                    ])

                    for sensor in self.sensors:
                        deltas = self.sync_delta_stats_ms.get(sensor, [])
                        ages = self.sync_age_stats_ms.get(sensor, [])
                        counts = self.sync_status_count.get(sensor, {})

                        writer.writerow([
                            sensor,
                            self.sample_count.get(sensor, 0),
                            counts.get("valid", 0),
                            counts.get("stale", 0),
                            counts.get("missing", 0),
                            f"{mean(deltas):.3f}" if deltas else "",
                            f"{median(deltas):.3f}" if deltas else "",
                            f"{max(deltas):.3f}" if deltas else "",
                            f"{mean(ages):.3f}" if ages else "",
                            f"{median(ages):.3f}" if ages else "",
                            f"{max(ages):.3f}" if ages else "",
                        ])

            except Exception as e:
                self.get_logger().error(f"Failed writing sync quality summary: {e}")

    # ============================================================
    # SENSOR CALLBACKS
    # ============================================================
    def gps_callback(self, msg):
        self.last_latitude = msg.latitude
        self.last_longitude = msg.longitude
        self.last_gps_altitude = msg.altitude
        self.last_gps_status = msg.status.status
        self.last_gps_service = msg.status.service

        payload = {
            "latitude": msg.latitude,
            "longitude": msg.longitude,
            "altitude": msg.altitude,
            "status": msg.status.status,
            "service": msg.status.service,
        }
        self.push_sensor_sample("gps", payload, self.header_stamp_to_ns(msg))

    def imu_callback(self, msg):
        payload = {
            "acc_x": msg.linear_acceleration.x,
            "acc_y": msg.linear_acceleration.y,
            "acc_z": msg.linear_acceleration.z,
        }
        self.push_sensor_sample("imu", payload, self.header_stamp_to_ns(msg))

    def ctd_callback(self, msg):
        data = list(msg.data)
        payload = {
            "depth": data[0] if len(data) > 0 else math.nan,
            "temp": data[1] if len(data) > 1 else math.nan,
            "cond": data[2] if len(data) > 2 else math.nan,
            "salinity": data[3] if len(data) > 3 else math.nan,
            "density": data[4] if len(data) > 4 else math.nan,
            "soundvel": data[5] if len(data) > 5 else math.nan,
        }
        self.push_sensor_sample("ctd", payload, None)

    def adcp_callback(self, msg):
        data = list(msg.data)

        if len(data) >= 10:
            payload = {
                "num_cells": int(data[0]),
                "num_beams": int(data[1]),
                "cell_size_m": data[2],
                "blanking_distance_m": data[3],
                "heading_deg": data[4],
                "pitch_deg": data[5],
                "roll_deg": data[6],
                "temperature_c": data[7],
                "salinity_psu": data[8],
                "pressure_dbar": data[9],
                "velocity_profile": ";".join(map(str, data[10:])),
            }
        else:
            payload = {
                "num_cells": "",
                "num_beams": "",
                "cell_size_m": "",
                "blanking_distance_m": "",
                "heading_deg": "",
                "pitch_deg": "",
                "roll_deg": "",
                "temperature_c": "",
                "salinity_psu": "",
                "pressure_dbar": "",
                "velocity_profile": ";".join(map(str, data)),
            }

        self.push_sensor_sample("adcp", payload, None)

    def sbes_callback(self, msg):
        data = list(msg.data)
        payload = {
            "depth": data[0] if len(data) > 0 else math.nan,
            "water_temp": data[1] if len(data) > 1 else math.nan,
            "quality_flag": data[2] if len(data) > 2 else math.nan,
        }
        self.push_sensor_sample("sbes", payload, None)

    def battery_callback(self, msg):
        voltage = msg.voltage
        current = msg.current
        percentage = float(msg.percentage)

        if math.isnan(percentage):
            percentage_percent = math.nan
        elif percentage <= 1.0:
            percentage_percent = percentage * 100.0
        else:
            percentage_percent = percentage

        self.last_battery_voltage = voltage
        self.last_battery_current = current
        self.last_battery_percent = percentage_percent

        payload = {
            "voltage_v": voltage,
            "current_a": current,
            "percentage_percent": percentage_percent,
        }
        self.push_sensor_sample("battery", payload, self.header_stamp_to_ns(msg))

    # ============================================================
    # MISSION CALLBACKS
    # ============================================================
    def waypoints_callback(self, msg):
        self.last_waypoints_msg = msg
        self.current_waypoint_seq = int(msg.current_seq)
        self.last_waypoint_count = len(msg.waypoints)

        if not self.logging_active:
            return

        signature = self.get_waypoints_signature(msg)
        if signature == self.last_waypoints_signature:
            return

        self.last_waypoints_signature = signature
        self.dump_waypoints(msg)
        self.write_mission_event("WAYPOINT_LIST_UPDATED", f"count={len(msg.waypoints)}, current_seq={msg.current_seq}")

    def waypoint_reached_callback(self, msg):
        self.last_reached_waypoint_seq = int(msg.wp_seq)

        if not self.logging_active:
            return

        ros_time = self.ros_stamp_to_float(msg.header.stamp)
        row = [
            self.get_local_timestamp(),
            f"{self.get_unix_time():.6f}",
            f"{self.get_mission_elapsed_sec():.3f}",
            f"{ros_time:.9f}",
            msg.header.frame_id,
            int(msg.wp_seq),
            self.last_flight_mode,
            self.last_flight_mode_class,
            self.safe_value(self.last_groundspeed),
            self.safe_value(self.last_heading),
            self.safe_value(self.last_latitude),
            self.safe_value(self.last_longitude),
        ]

        for writer in self.mission_waypoint_reached_writers:
            writer.writerow(row)

        self.write_mission_event("WAYPOINT_REACHED", f"wp_seq={msg.wp_seq}", ros_time=ros_time)

    def vfr_hud_callback(self, msg):
        self.last_airspeed = msg.airspeed
        self.last_groundspeed = msg.groundspeed
        self.last_heading = msg.heading
        self.last_throttle = msg.throttle
        self.last_vfr_altitude = msg.altitude
        self.last_climb = msg.climb

    def statustext_callback(self, msg):
        if not self.logging_active:
            return

        ros_time = self.ros_stamp_to_float(msg.header.stamp)
        text = str(msg.text)
        severity = int(msg.severity)

        row = [
            self.get_local_timestamp(),
            f"{self.get_unix_time():.6f}",
            f"{self.get_mission_elapsed_sec():.3f}",
            f"{ros_time:.9f}",
            msg.header.frame_id,
            severity,
            text,
            self.last_flight_mode,
            self.last_flight_mode_class,
        ]

        for writer in self.mission_statustext_writers:
            writer.writerow(row)

        text_upper = text.upper()
        if "RTL" in text_upper or "RTH" in text_upper or "RETURN" in text_upper or "FAILSAFE" in text_upper:
            self.write_mission_event("AUTOPILOT_STATUS_ALERT", text, ros_time=ros_time)

    # ============================================================
    # MISSION FILE SETUP
    # ============================================================
    def reset_user_friendly_mission_stats(self):
        self.mission_event_count = 0
        self.mission_return_home_detected = False
        self.mission_failsafe_detected = False
        self.mission_max_speed_mps = 0.0
        self.mission_speed_sum_mps = 0.0
        self.mission_speed_sample_count = 0
        self.mission_distance_m = 0.0
        self.mission_last_distance_lat = None
        self.mission_last_distance_lon = None
        self.last_mission_timeline_slot = -1
        self.last_mission_timeline_write_time = 0.0
        self.mission_start_battery_percent = self.last_battery_percent
        self.mission_end_battery_percent = math.nan

        self.internet_online = None
        self.cloud_online = None
        self.internet_lost_start_time = None
        self.cloud_lost_start_time = None
        self.internet_lost_count = 0
        self.cloud_lost_count = 0
        self.internet_total_down_sec = 0.0
        self.cloud_total_down_sec = 0.0
        self.last_internet_write_slot = -1

    def init_mission_logger(self):
        for base_path in self.base_paths:
            mission_dir = os.path.join(base_path, "mission")
            os.makedirs(mission_dir, exist_ok=True)

            readme_path = os.path.join(mission_dir, "00_READ_ME_FIRST.txt")
            log_path = os.path.join(mission_dir, "mission_events_readable.log")

            with open(readme_path, "w") as f:
                f.write("SEANO Mission Log - Quick Guide\n")
                f.write("================================\n\n")
                f.write("Root files:\n")
                f.write("- mission_summary.txt\n")
                f.write("- mission_info.txt\n")
                f.write("- system_metrics.csv\n\n")
                f.write("Mission files:\n")
                f.write("- mission_readable.csv\n")
                f.write("- mission_timeline.csv\n")
                f.write("- mission_events.csv\n")
                f.write("- mission_events_readable.log\n")
                f.write("- mission_state_changes.csv\n")
                f.write("- mission_waypoint_reached.csv\n")
                f.write("- mission_waypoints.csv\n")
                f.write("- mission_statustext.csv\n")
                f.write("- internet_status.csv\n")
                f.write("- internet_events.log\n\n")
                f.write("Sensor files:\n")
                f.write("- sensor/synchronized_log.csv          : data sensor hasil matching sinkron\n")
                f.write("- sensor/sync_diagnostics.csv          : delay, status, dan kualitas tiap frame\n")
                f.write("- sensor/sync_quality_summary.csv      : ringkasan kualitas sinkronisasi\n")
                f.write("- sensor/gps.csv\n")
                f.write("- sensor/imu.csv\n")
                f.write("- sensor/ctd.csv\n")
                f.write("- sensor/adcp.csv\n")
                f.write("- sensor/sbes.csv\n")
                f.write("- sensor/battery.csv\n")

            mission_log_file = open(log_path, "w", buffering=1)
            mission_log_file.write("SEANO Mission Events\n")
            mission_log_file.write("====================\n")
            self.mission_log_files.append(mission_log_file)

            internet_status_file = open(os.path.join(mission_dir, "internet_status.csv"), "w", newline="", buffering=1)
            internet_status_writer = csv.writer(internet_status_file)
            internet_status_writer.writerow([
                "time",
                "elapsed_sec",
                "internet",
                "cloud_mqtt",
                "internet_lost_count",
                "cloud_lost_count",
                "internet_total_down_sec",
                "cloud_total_down_sec",
                "note",
            ])

            internet_event_file = open(os.path.join(mission_dir, "internet_events.log"), "w", buffering=1)
            internet_event_file.write("SEANO Internet Events\n")
            internet_event_file.write("=====================\n")

            self.internet_status_files.append(internet_status_file)
            self.internet_status_writers.append(internet_status_writer)
            self.internet_event_files.append(internet_event_file)

            self.open_mission_csv(
                os.path.join(mission_dir, "mission_readable.csv"),
                [
                    "time",
                    "elapsed_sec",
                    "status",
                    "mode",
                    "mode_class",
                    "current_wp",
                    "last_reached_wp",
                    "wp_count",
                    "speed_mps",
                    "heading_deg",
                    "throttle",
                    "lat",
                    "lon",
                    "gps_alt",
                    "battery_percent",
                    "internet",
                    "cloud_mqtt",
                    "note",
                ],
                self.mission_readable_files,
                self.mission_readable_writers,
            )

            self.open_mission_csv(
                os.path.join(mission_dir, "mission_timeline.csv"),
                [
                    "local_timestamp",
                    "unix_time",
                    "mission_elapsed_sec",
                    "connected",
                    "armed",
                    "guided",
                    "manual_input",
                    "mode",
                    "mode_class",
                    "system_status",
                    "current_waypoint_seq",
                    "last_reached_waypoint_seq",
                    "waypoint_count",
                    "groundspeed_mps",
                    "airspeed_mps",
                    "heading_deg",
                    "throttle",
                    "vfr_altitude",
                    "climb",
                    "latitude",
                    "longitude",
                    "gps_altitude",
                    "gps_status",
                    "gps_service",
                    "battery_voltage",
                    "battery_current",
                    "battery_percent",
                    "internet",
                    "cloud_mqtt",
                ],
                self.mission_timeline_files,
                self.mission_timeline_writers,
            )

            self.open_mission_csv(
                os.path.join(mission_dir, "mission_events.csv"),
                [
                    "local_timestamp",
                    "unix_time",
                    "mission_elapsed_sec",
                    "ros_time",
                    "event_type",
                    "detail",
                    "mode",
                    "mode_class",
                    "current_waypoint_seq",
                    "last_reached_waypoint_seq",
                    "groundspeed_mps",
                    "heading_deg",
                    "latitude",
                    "longitude",
                ],
                self.mission_events_files,
                self.mission_events_writers,
            )

            self.open_mission_csv(
                os.path.join(mission_dir, "mission_state_changes.csv"),
                [
                    "local_timestamp",
                    "unix_time",
                    "mission_elapsed_sec",
                    "old_mode",
                    "new_mode",
                    "old_mode_class",
                    "new_mode_class",
                    "connected",
                    "armed",
                    "guided",
                    "manual_input",
                ],
                self.mission_state_files,
                self.mission_state_writers,
            )

            self.open_mission_csv(
                os.path.join(mission_dir, "mission_waypoint_reached.csv"),
                [
                    "local_timestamp",
                    "unix_time",
                    "mission_elapsed_sec",
                    "ros_time",
                    "frame_id",
                    "wp_seq",
                    "mode",
                    "mode_class",
                    "groundspeed_mps",
                    "heading_deg",
                    "latitude",
                    "longitude",
                ],
                self.mission_waypoint_reached_files,
                self.mission_waypoint_reached_writers,
            )

            self.open_mission_csv(
                os.path.join(mission_dir, "mission_waypoints.csv"),
                [
                    "dump_local_timestamp",
                    "dump_unix_time",
                    "dump_mission_elapsed_sec",
                    "current_seq",
                    "waypoint_count",
                    "index",
                    "is_current",
                    "autocontinue",
                    "frame",
                    "command",
                    "param1",
                    "param2",
                    "param3",
                    "param4",
                    "x_lat",
                    "y_long",
                    "z_alt",
                ],
                self.mission_waypoints_files,
                self.mission_waypoints_writers,
            )

            self.open_mission_csv(
                os.path.join(mission_dir, "mission_statustext.csv"),
                [
                    "local_timestamp",
                    "unix_time",
                    "mission_elapsed_sec",
                    "ros_time",
                    "frame_id",
                    "severity",
                    "text",
                    "mode",
                    "mode_class",
                ],
                self.mission_statustext_files,
                self.mission_statustext_writers,
            )

    def open_mission_csv(self, path, header, file_list, writer_list):
        file_obj = open(path, "w", newline="", buffering=1)
        writer = csv.writer(file_obj)
        writer.writerow(header)
        file_list.append(file_obj)
        writer_list.append(writer)

    # ============================================================
    # SYSTEM METRICS
    # ============================================================
    def init_system_metrics_logger(self):
        for base_path in self.base_paths:
            metrics_path = os.path.join(base_path, "system_metrics.csv")
            metrics_file = open(metrics_path, "w", newline="", buffering=1)
            metrics_writer = csv.writer(metrics_file)

            # Format dikembalikan seperti logger lama.
            metrics_writer.writerow([
                "timestamp",
                "write_speed_Bps",
                "cpu_percent",
                "ram_percent",
                "jetson_temp_c",
                "gps_hz",
                "imu_hz",
                "ctd_hz",
                "adcp_hz",
                "battery_hz",
                "sbes_hz",
                "mission_hz",
            ])

            self.system_metrics_files.append(metrics_file)
            self.system_metrics_writers.append(metrics_writer)

    def get_sensor_rate(self, key):
        last_ns = self.last_sensor_rx_wall_ns.get(key, 0)
        if last_ns <= 0:
            return 0.0

        age_sec = (self.wall_ns() - last_ns) / 1e9
        return 1.0 if age_sec <= 1.5 else 0.0

    def get_mission_timeline_rate(self):
        if self.last_mission_timeline_write_time <= 0.0:
            return 0.0

        age_sec = time.time() - self.last_mission_timeline_write_time
        return 1.0 if age_sec <= 1.5 else 0.0

    def log_periodic_metrics(self):
        if not self.logging_active or not self.system_metrics_writers:
            self.bytes_written_since_last_metrics = 0
            self.last_metrics_time = time.time()
            return

        now = time.time()
        elapsed = now - self.last_metrics_time if self.last_metrics_time else self.metrics_interval
        if elapsed <= 0:
            elapsed = self.metrics_interval

        write_speed = self.bytes_written_since_last_metrics / elapsed
        cpu_usage = psutil.cpu_percent(interval=None)
        ram_usage = psutil.virtual_memory().percent

        jetson_temp_c = self.read_jetson_temperature()
        jetson_temp_str = f"{jetson_temp_c:.2f}" if jetson_temp_c is not None else "nan"

        row = [
            self.get_local_timestamp(),
            f"{write_speed:.2f}",
            f"{cpu_usage:.2f}",
            f"{ram_usage:.2f}",
            jetson_temp_str,
            f"{self.get_sensor_rate('gps'):.2f}",
            f"{self.get_sensor_rate('imu'):.2f}",
            f"{self.get_sensor_rate('ctd'):.2f}",
            f"{self.get_sensor_rate('adcp'):.2f}",
            f"{self.get_sensor_rate('battery'):.2f}",
            f"{self.get_sensor_rate('sbes'):.2f}",
            f"{self.get_mission_timeline_rate():.2f}",
        ]

        for writer in self.system_metrics_writers:
            try:
                writer.writerow(row)
            except Exception as e:
                self.get_logger().error(f"Metrics logging failed: {e}")

        self.bytes_written_since_last_metrics = 0
        self.last_metrics_time = now

    def read_jetson_temperature(self):
        preferred_sources = [
            "CPU-therm",
            "cpu-therm",
            "Tdiode_tegra",
            "Tboard_tegra",
            "GPU-therm",
            "gpu-therm",
            "SOC0-therm",
            "soc0-therm",
        ]

        valid_entries = []

        for zone_path in glob.glob("/sys/class/thermal/thermal_zone*"):
            type_path = os.path.join(zone_path, "type")
            temp_path = os.path.join(zone_path, "temp")

            try:
                if not os.path.exists(type_path) or not os.path.exists(temp_path):
                    continue

                with open(type_path, "r") as f:
                    zone_type = f.read().strip()

                with open(temp_path, "r") as f:
                    raw_temp = f.read().strip()

                temp_value = float(raw_temp)
                temp_c = temp_value / 1000.0 if abs(temp_value) > 1000.0 else temp_value
                valid_entries.append((zone_type, temp_c))

            except Exception:
                continue

        if not valid_entries:
            return None

        for preferred in preferred_sources:
            for zone_type, temp_c in valid_entries:
                if zone_type == preferred:
                    if self.jetson_temp_source != zone_type:
                        self.jetson_temp_source = zone_type
                        self.get_logger().info(f"Jetson temperature source: {zone_type}")
                    return temp_c

        zone_type, temp_c = valid_entries[0]
        if self.jetson_temp_source != zone_type:
            self.jetson_temp_source = zone_type
            self.get_logger().info(f"Jetson temperature source: {zone_type}")

        return temp_c

    # ============================================================
    # INTERNET MONITOR
    # ============================================================
    def tcp_probe(self, host, port, timeout_sec=0.4):
        try:
            with socket.create_connection((host, port), timeout=timeout_sec):
                return True
        except Exception:
            return False

    def internet_text(self, value):
        if value is None:
            return "UNKNOWN"
        return "ONLINE" if value else "OFFLINE"

    def write_internet_event(self, text):
        if not self.logging_active:
            return

        line = f"{self.get_local_timestamp()} | t={self.get_mission_elapsed_sec():.1f}s | {text}\n"

        for f in self.internet_event_files:
            try:
                f.write(line)
                f.flush()
            except Exception:
                pass

        try:
            self.write_mission_event("INTERNET_EVENT", text)
        except Exception:
            pass

    def write_internet_status_row(self, note="OK"):
        if not self.logging_active:
            return

        current_slot = int(time.time())
        if current_slot == self.last_internet_write_slot:
            return
        self.last_internet_write_slot = current_slot

        row = [
            self.get_local_timestamp(),
            f"{self.get_mission_elapsed_sec():.1f}",
            self.internet_text(self.internet_online),
            self.internet_text(self.cloud_online),
            self.internet_lost_count,
            self.cloud_lost_count,
            f"{self.get_current_total_internet_down_sec():.1f}",
            f"{self.get_current_total_cloud_down_sec():.1f}",
            note,
        ]

        for writer in self.internet_status_writers:
            try:
                writer.writerow(row)
            except Exception:
                pass

    def monitor_internet_simple(self):
        if not self.logging_active:
            return

        internet_now = self.tcp_probe(self.internet_probe_host, self.internet_probe_port, self.internet_probe_timeout)
        cloud_now = self.tcp_probe(self.cloud_probe_host, self.cloud_probe_port, self.cloud_probe_timeout)

        now = time.time()
        note = "OK"

        if self.internet_online is None:
            self.internet_online = internet_now
            if internet_now:
                self.write_internet_event("INTERNET AWAL: ONLINE")
            else:
                self.internet_lost_start_time = now
                self.internet_lost_count += 1
                note = "INTERNET MATI"
                self.write_internet_event("INTERNET MATI sejak awal logging")
        elif self.internet_online and not internet_now:
            self.internet_online = False
            self.internet_lost_start_time = now
            self.internet_lost_count += 1
            note = "INTERNET MATI"
            self.write_internet_event("INTERNET MATI")
        elif not self.internet_online and internet_now:
            self.internet_online = True
            down_sec = 0.0
            if self.internet_lost_start_time is not None:
                down_sec = now - self.internet_lost_start_time
                self.internet_total_down_sec += down_sec
            note = f"INTERNET NYALA LAGI setelah {down_sec:.1f} detik"
            self.write_internet_event(note)
            self.internet_lost_start_time = None

        if self.cloud_online is None:
            self.cloud_online = cloud_now
            if cloud_now:
                self.write_internet_event("MQTT CLOUD AWAL: ONLINE")
            else:
                self.cloud_lost_start_time = now
                self.cloud_lost_count += 1
                if note == "OK":
                    note = "MQTT CLOUD MATI"
                self.write_internet_event("MQTT CLOUD MATI sejak awal logging")
        elif self.cloud_online and not cloud_now:
            self.cloud_online = False
            self.cloud_lost_start_time = now
            self.cloud_lost_count += 1
            if note == "OK":
                note = "MQTT CLOUD MATI"
            self.write_internet_event("MQTT CLOUD MATI")
        elif not self.cloud_online and cloud_now:
            self.cloud_online = True
            down_sec = 0.0
            if self.cloud_lost_start_time is not None:
                down_sec = now - self.cloud_lost_start_time
                self.cloud_total_down_sec += down_sec
            cloud_note = f"MQTT CLOUD NYALA LAGI setelah {down_sec:.1f} detik"
            if note == "OK":
                note = cloud_note
            self.write_internet_event(cloud_note)
            self.cloud_lost_start_time = None

        self.write_internet_status_row(note)

    def get_current_total_internet_down_sec(self):
        total = self.internet_total_down_sec
        if self.internet_online is False and self.internet_lost_start_time is not None:
            total += time.time() - self.internet_lost_start_time
        return total

    def get_current_total_cloud_down_sec(self):
        total = self.cloud_total_down_sec
        if self.cloud_online is False and self.cloud_lost_start_time is not None:
            total += time.time() - self.cloud_lost_start_time
        return total

    # ============================================================
    # MISSION WRITE HELPERS
    # ============================================================
    def write_mission_event(self, event_type, detail, ros_time=None):
        if not self.logging_active:
            return

        self.mission_event_count += 1

        event_upper = str(event_type).upper()
        detail_upper = str(detail).upper()

        if "RETURN" in event_upper or "RTL" in detail_upper or "RTH" in detail_upper or "RETURN" in detail_upper:
            self.mission_return_home_detected = True

        if "FAILSAFE" in event_upper or "FAILSAFE" in detail_upper:
            self.mission_failsafe_detected = True

        row = [
            self.get_local_timestamp(),
            f"{self.get_unix_time():.6f}",
            f"{self.get_mission_elapsed_sec():.3f}",
            "" if ros_time is None else f"{ros_time:.9f}",
            event_type,
            detail,
            self.last_flight_mode,
            self.last_flight_mode_class,
            self.current_waypoint_seq,
            self.last_reached_waypoint_seq,
            self.safe_value(self.last_groundspeed),
            self.safe_value(self.last_heading),
            self.safe_value(self.last_latitude),
            self.safe_value(self.last_longitude),
        ]

        for writer in self.mission_events_writers:
            writer.writerow(row)

        human_line = (
            f"{self.get_local_timestamp()} | "
            f"t={self.get_mission_elapsed_sec():.1f}s | "
            f"{event_type}: {detail} | "
            f"mode={self.last_flight_mode} | "
            f"wp={self.current_waypoint_seq} | "
            f"speed={self.safe_value(self.last_groundspeed)} | "
            f"pos={self.safe_value(self.last_latitude)},{self.safe_value(self.last_longitude)}\n"
        )

        for log_file in self.mission_log_files:
            log_file.write(human_line)
            log_file.flush()

    def write_mission_state_change(self, old_mode, new_mode, old_mode_class, new_mode_class):
        if not self.logging_active:
            return

        row = [
            self.get_local_timestamp(),
            f"{self.get_unix_time():.6f}",
            f"{self.get_mission_elapsed_sec():.3f}",
            old_mode,
            new_mode,
            old_mode_class,
            new_mode_class,
            self.last_connected_state,
            self.last_armed_state,
            self.last_guided_state,
            self.last_manual_input_state,
        ]

        for writer in self.mission_state_writers:
            writer.writerow(row)

    def get_waypoints_signature(self, msg):
        parts = [str(msg.current_seq), str(len(msg.waypoints))]
        for wp in msg.waypoints:
            parts.append(":".join([
                str(wp.frame),
                str(wp.command),
                f"{wp.x_lat:.8f}",
                f"{wp.y_long:.8f}",
                f"{wp.z_alt:.3f}",
            ]))
        return "|".join(parts)

    def dump_waypoints(self, msg):
        if not self.logging_active:
            return

        dump_time = self.get_local_timestamp()
        dump_unix = f"{self.get_unix_time():.6f}"
        dump_elapsed = f"{self.get_mission_elapsed_sec():.3f}"
        waypoint_count = len(msg.waypoints)

        for idx, wp in enumerate(msg.waypoints):
            row = [
                dump_time,
                dump_unix,
                dump_elapsed,
                int(msg.current_seq),
                waypoint_count,
                idx,
                wp.is_current,
                wp.autocontinue,
                wp.frame,
                wp.command,
                wp.param1,
                wp.param2,
                wp.param3,
                wp.param4,
                wp.x_lat,
                wp.y_long,
                wp.z_alt,
            ]
            for writer in self.mission_waypoints_writers:
                writer.writerow(row)

    def log_mission_timeline(self):
        if not self.logging_active:
            return

        current_slot = int(time.time())
        if current_slot == self.last_mission_timeline_slot:
            return

        self.last_mission_timeline_slot = current_slot
        self.last_mission_timeline_write_time = time.time()

        speed = self.last_groundspeed
        if isinstance(speed, float) and not math.isnan(speed):
            self.mission_max_speed_mps = max(self.mission_max_speed_mps, speed)
            self.mission_speed_sum_mps += speed
            self.mission_speed_sample_count += 1

        self.update_mission_distance()

        timeline_row = [
            self.get_local_timestamp(),
            f"{self.get_unix_time():.6f}",
            f"{self.get_mission_elapsed_sec():.3f}",
            self.last_connected_state,
            self.last_armed_state,
            self.last_guided_state,
            self.last_manual_input_state,
            self.last_flight_mode,
            self.last_flight_mode_class,
            self.last_system_status,
            self.current_waypoint_seq,
            self.last_reached_waypoint_seq,
            self.last_waypoint_count,
            self.safe_value(self.last_groundspeed),
            self.safe_value(self.last_airspeed),
            self.safe_value(self.last_heading),
            self.safe_value(self.last_throttle),
            self.safe_value(self.last_vfr_altitude),
            self.safe_value(self.last_climb),
            self.safe_value(self.last_latitude),
            self.safe_value(self.last_longitude),
            self.safe_value(self.last_gps_altitude),
            self.safe_value(self.last_gps_status),
            self.safe_value(self.last_gps_service),
            self.safe_value(self.last_battery_voltage),
            self.safe_value(self.last_battery_current),
            self.safe_value(self.last_battery_percent),
            self.internet_text(self.internet_online),
            self.internet_text(self.cloud_online),
        ]

        for writer in self.mission_timeline_writers:
            writer.writerow(timeline_row)

        readable_row = [
            self.get_local_timestamp(),
            f"{self.get_mission_elapsed_sec():.1f}",
            "ACTIVE" if self.last_armed_state else "STANDBY",
            self.last_flight_mode,
            self.last_flight_mode_class,
            self.current_waypoint_seq,
            self.last_reached_waypoint_seq,
            self.last_waypoint_count,
            self.safe_value(self.last_groundspeed),
            self.safe_value(self.last_heading),
            self.safe_value(self.last_throttle),
            self.safe_value(self.last_latitude),
            self.safe_value(self.last_longitude),
            self.safe_value(self.last_gps_altitude),
            self.safe_value(self.last_battery_percent),
            self.internet_text(self.internet_online),
            self.internet_text(self.cloud_online),
            self.make_readable_note(),
        ]

        for writer in self.mission_readable_writers:
            writer.writerow(readable_row)

    def make_readable_note(self):
        if self.internet_online is False:
            return "INTERNET OFFLINE"
        if self.cloud_online is False:
            return "MQTT CLOUD OFFLINE"
        if self.last_flight_mode_class == "RETURN_HOME":
            return "RETURN HOME / RTL / RTH detected"
        if self.last_reached_waypoint_seq >= 0:
            return f"Last reached waypoint {self.last_reached_waypoint_seq}"
        return ""

    def update_mission_distance(self):
        lat = self.last_latitude
        lon = self.last_longitude

        if not isinstance(lat, float) or not isinstance(lon, float):
            return

        if math.isnan(lat) or math.isnan(lon):
            return

        if self.mission_last_distance_lat is None or self.mission_last_distance_lon is None:
            self.mission_last_distance_lat = lat
            self.mission_last_distance_lon = lon
            return

        step = self.haversine_m(self.mission_last_distance_lat, self.mission_last_distance_lon, lat, lon)

        if 0.0 <= step <= 50.0:
            self.mission_distance_m += step
            self.mission_last_distance_lat = lat
            self.mission_last_distance_lon = lon

    def haversine_m(self, lat1, lon1, lat2, lon2):
        radius_m = 6371000.0
        p1 = math.radians(lat1)
        p2 = math.radians(lat2)
        dp = math.radians(lat2 - lat1)
        dl = math.radians(lon2 - lon1)

        a = math.sin(dp / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2.0) ** 2
        c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
        return radius_m * c

    def write_mission_summary(self, reason, end_time_obj, elapsed):
        avg_speed = 0.0
        if self.mission_speed_sample_count > 0:
            avg_speed = self.mission_speed_sum_mps / self.mission_speed_sample_count

        self.mission_end_battery_percent = self.last_battery_percent
        internet_total_down = self.get_current_total_internet_down_sec()
        cloud_total_down = self.get_current_total_cloud_down_sec()

        for base_path in self.base_paths:
            summary_path = os.path.join(base_path, "mission_summary.txt")
            self.summary_paths.append(summary_path)

            try:
                with open(summary_path, "w") as f:
                    f.write("SEANO Mission Summary\n")
                    f.write("=====================\n\n")
                    f.write(f"Mission ID              : {self.mission_id}\n")
                    f.write(f"Start time              : {self.start_time_obj}\n")
                    f.write(f"End time                : {end_time_obj}\n")
                    f.write(f"Duration                : {elapsed:.1f} sec ({elapsed / 60.0:.2f} min)\n")
                    f.write(f"Stop reason             : {reason}\n\n")

                    f.write("\nFinal state\n")
                    f.write("-----------\n")
                    f.write(f"Connected               : {self.last_connected_state}\n")
                    f.write(f"Armed                   : {self.last_armed_state}\n")
                    f.write(f"Mode                    : {self.last_flight_mode}\n")
                    f.write(f"Mode class              : {self.last_flight_mode_class}\n")
                    f.write(f"Current waypoint        : {self.current_waypoint_seq}\n")
                    f.write(f"Last reached waypoint   : {self.last_reached_waypoint_seq}\n")
                    f.write(f"Waypoint count          : {self.last_waypoint_count}\n\n")

                    f.write("Movement\n")
                    f.write("--------\n")
                    f.write(f"Max speed               : {self.mission_max_speed_mps:.3f} m/s\n")
                    f.write(f"Average speed           : {avg_speed:.3f} m/s\n")
                    f.write(f"Estimated distance      : {self.mission_distance_m:.2f} m\n")
                    f.write(f"Last latitude           : {self.safe_value(self.last_latitude)}\n")
                    f.write(f"Last longitude          : {self.safe_value(self.last_longitude)}\n")
                    f.write(f"Last heading            : {self.safe_value(self.last_heading)} deg\n\n")

                    f.write("Battery\n")
                    f.write("-------\n")
                    f.write(f"Start battery           : {self.safe_value(self.mission_start_battery_percent)} %\n")
                    f.write(f"End battery             : {self.safe_value(self.mission_end_battery_percent)} %\n")
                    f.write(f"Last voltage            : {self.safe_value(self.last_battery_voltage)} V\n")
                    f.write(f"Last current            : {self.safe_value(self.last_battery_current)} A\n\n")

                    f.write("Internet\n")
                    f.write("--------\n")
                    f.write(f"Internet final status   : {self.internet_text(self.internet_online)}\n")
                    f.write(f"Internet lost count     : {self.internet_lost_count}\n")
                    f.write(f"Internet total down     : {internet_total_down:.1f} sec\n")
                    f.write(f"MQTT final status       : {self.internet_text(self.cloud_online)}\n")
                    f.write(f"MQTT lost count         : {self.cloud_lost_count}\n")
                    f.write(f"MQTT total down         : {cloud_total_down:.1f} sec\n\n")

                    f.write("Events\n")
                    f.write("------\n")
                    f.write(f"Event count             : {self.mission_event_count}\n")
                    f.write(f"Return-home detected    : {self.mission_return_home_detected}\n")
                    f.write(f"Failsafe detected       : {self.mission_failsafe_detected}\n\n")

                    f.write("Recommended files to open\n")
                    f.write("-------------------------\n")
                    f.write("1. sensor/synchronized_log.xlsx\n")
                    f.write("2. sensor/synchronized_log.csv\n")
                    f.write("3. sensor/sync_diagnostics.csv\n")
                    f.write("4. sensor/sync_quality_summary.csv\n")
                    f.write("5. system_metrics.csv\n")
                    f.write("6. mission/mission_readable.csv\n")
                    f.write("7. mission/mission_events_readable.log\n")
                    f.write("8. video/\n")
            except Exception as e:
                self.get_logger().error(f"Failed writing mission summary: {e}")


    # ============================================================
    # EXCEL EXPORT
    # ============================================================
    def export_synchronized_workbooks(self):
        """
        Membuat satu file Excel multi-sheet setelah misi selesai.

        File output:
        sensor/synchronized_log.xlsx

        Sheet:
        - Sync_Data        : data semua sensor yang sudah disinkronkan
        - GPS_Sync         : data GPS hasil sinkronisasi per sync_time
        - IMU_Sync         : data IMU hasil sinkronisasi per sync_time
        - CTD_Sync         : data CTD hasil sinkronisasi per sync_time
        - ADCP_Sync        : data ADCP hasil sinkronisasi per sync_time
        - SBES_Sync        : data SBES hasil sinkronisasi per sync_time
        - Battery_Sync     : data baterai hasil sinkronisasi per sync_time
        - Sync_Diagnostics : delay/status/stale/missing
        - Sync_Quality     : ringkasan kualitas sinkronisasi

        Catatan:
        Sheet GPS_Sync sampai Battery_Sync BUKAN file sensor 1 Hz mentah.
        Sheet tersebut dibuat dari synchronized_log.csv sehingga seluruh barisnya
        sudah mengikuti sync_time yang sama.
        """
        for base_path in self.base_paths:
            sensor_dir = os.path.join(base_path, "sensor")
            if not os.path.isdir(sensor_dir):
                continue

            sync_csv = os.path.join(sensor_dir, "synchronized_log.csv")
            diagnostics_csv = os.path.join(sensor_dir, "sync_diagnostics.csv")
            quality_csv = os.path.join(sensor_dir, "sync_quality_summary.csv")

            temp_sources = []

            try:
                temp_sources = self.create_synced_sensor_sheet_csvs(sensor_dir, sync_csv)

                sources = [
                    ("Sync_Data", sync_csv),
                    ("GPS_Sync", temp_sources[0][1] if len(temp_sources) > 0 else ""),
                    ("IMU_Sync", temp_sources[1][1] if len(temp_sources) > 1 else ""),
                    ("CTD_Sync", temp_sources[2][1] if len(temp_sources) > 2 else ""),
                    ("ADCP_Sync", temp_sources[3][1] if len(temp_sources) > 3 else ""),
                    ("SBES_Sync", temp_sources[4][1] if len(temp_sources) > 4 else ""),
                    ("Battery_Sync", temp_sources[5][1] if len(temp_sources) > 5 else ""),
                    ("Sync_Diagnostics", diagnostics_csv),
                    ("Sync_Quality", quality_csv),
                ]

                sources = [(name, path) for name, path in sources if path and os.path.exists(path)]

                if not sources:
                    continue

                xlsx_path = os.path.join(sensor_dir, "synchronized_log.xlsx")
                self.write_xlsx_from_csv_sources(xlsx_path, sources)
                self.get_logger().info(f"Excel synchronized workbook created: {xlsx_path}")

            except Exception as e:
                self.get_logger().error(f"Failed creating Excel synchronized workbook: {e}")

            finally:
                for _, temp_path in temp_sources:
                    try:
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                    except Exception:
                        pass

    def create_synced_sensor_sheet_csvs(self, sensor_dir, sync_csv):
        """
        Membuat CSV sementara untuk sheet per sensor yang sudah tersinkronkan.

        Input utama:
        - sensor/synchronized_log.csv

        Output sementara:
        - .xlsx_tmp_gps_sync.csv
        - .xlsx_tmp_imu_sync.csv
        - dst.

        File sementara dihapus setelah XLSX selesai dibuat.
        """
        temp_sources = []

        if not os.path.exists(sync_csv):
            return temp_sources

        sensor_sheet_names = {
            "gps": "GPS_Sync",
            "imu": "IMU_Sync",
            "ctd": "CTD_Sync",
            "adcp": "ADCP_Sync",
            "sbes": "SBES_Sync",
            "battery": "Battery_Sync",
        }

        with open(sync_csv, "r", newline="") as source_file:
            reader = csv.DictReader(source_file)
            fieldnames = reader.fieldnames or []

            rows = list(reader)

        base_columns = ["sync_time", "mission_elapsed_sec"]

        for sensor in self.sensors:
            sensor_columns = [
                col for col in fieldnames
                if col.startswith(f"{sensor}_")
            ]

            if not sensor_columns:
                continue

            temp_path = os.path.join(sensor_dir, f".xlsx_tmp_{sensor}_sync.csv")

            with open(temp_path, "w", newline="", buffering=1) as temp_file:
                writer = csv.writer(temp_file)

                header = base_columns + [
                    col[len(sensor) + 1:] for col in sensor_columns
                ]

                writer.writerow(header)

                for row in rows:
                    writer.writerow(
                        [row.get(col, "") for col in base_columns]
                        + [row.get(col, "") for col in sensor_columns]
                    )

            temp_sources.append((sensor_sheet_names.get(sensor, f"{sensor.upper()}_Sync"), temp_path))

        return temp_sources

    def xlsx_col_name(self, index):
        """Convert 1-based column index to Excel column name."""
        name = ""
        while index > 0:
            index, remainder = divmod(index - 1, 26)
            name = chr(65 + remainder) + name
        return name

    def clean_xml_text(self, value):
        if value is None:
            return ""

        text = str(value)

        # XML 1.0 tidak menerima control character tertentu.
        return "".join(
            ch for ch in text
            if ch == "\t" or ch == "\n" or ch == "\r" or ord(ch) >= 32
        )

    def safe_sheet_name(self, name, used_names):
        invalid = set('[]:*?/\\')
        cleaned = "".join("_" if ch in invalid else ch for ch in str(name))
        cleaned = cleaned.strip() or "Sheet"
        cleaned = cleaned[:31]

        base = cleaned
        counter = 1

        while cleaned in used_names:
            suffix = f"_{counter}"
            cleaned = (base[:31 - len(suffix)] + suffix)
            counter += 1

        used_names.add(cleaned)
        return cleaned

    def write_xlsx_from_csv_sources(self, xlsx_path, sources):
        """
        Minimal XLSX writer tanpa dependency openpyxl.
        Semua cell ditulis sebagai inline string agar kompatibel dengan Excel/LibreOffice.
        """
        used_names = set()
        sheets = []

        for idx, (sheet_name, csv_path) in enumerate(sources, start=1):
            safe_name = self.safe_sheet_name(sheet_name, used_names)
            sheet_file = f"worksheets/sheet{idx}.xml"
            sheets.append((idx, safe_name, csv_path, sheet_file))

        with zipfile.ZipFile(xlsx_path, "w", zipfile.ZIP_DEFLATED) as zf:
            self.xlsx_write_content_types(zf, len(sheets))
            self.xlsx_write_root_rels(zf)
            self.xlsx_write_workbook(zf, sheets)
            self.xlsx_write_workbook_rels(zf, sheets)

            for sheet_id, sheet_name, csv_path, sheet_file in sheets:
                self.xlsx_write_sheet_from_csv(
                    zf,
                    f"xl/{sheet_file}",
                    csv_path,
                )

    def xlsx_write_content_types(self, zf, sheet_count):
        sheet_overrides = []

        for idx in range(1, sheet_count + 1):
            sheet_overrides.append(
                f'<Override PartName="/xl/worksheets/sheet{idx}.xml" '
                f'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            )

        xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/xl/workbook.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
            + "".join(sheet_overrides) +
            '</Types>'
        )

        zf.writestr("[Content_Types].xml", xml)

    def xlsx_write_root_rels(self, zf):
        xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
            'Target="xl/workbook.xml"/>'
            '</Relationships>'
        )

        zf.writestr("_rels/.rels", xml)

    def xlsx_write_workbook(self, zf, sheets):
        sheet_xml = []

        for sheet_id, sheet_name, csv_path, sheet_file in sheets:
            sheet_xml.append(
                f'<sheet name="{xml_escape(sheet_name, quote=True)}" '
                f'sheetId="{sheet_id}" r:id="rId{sheet_id}"/>'
            )

        xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            '<sheets>'
            + "".join(sheet_xml) +
            '</sheets>'
            '</workbook>'
        )

        zf.writestr("xl/workbook.xml", xml)

    def xlsx_write_workbook_rels(self, zf, sheets):
        rels = []

        for sheet_id, sheet_name, csv_path, sheet_file in sheets:
            rels.append(
                f'<Relationship Id="rId{sheet_id}" '
                f'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
                f'Target="{sheet_file}"/>'
            )

        xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            + "".join(rels) +
            '</Relationships>'
        )

        zf.writestr("xl/_rels/workbook.xml.rels", xml)

    def xlsx_write_sheet_from_csv(self, zf, sheet_zip_path, csv_path):
        with zf.open(sheet_zip_path, "w") as sheet_file:
            sheet_file.write(
                b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                b'<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                b'<sheetData>'
            )

            try:
                with open(csv_path, "r", newline="", encoding="utf-8") as f:
                    reader = csv.reader(f)

                    for row_idx, row in enumerate(reader, start=1):
                        sheet_file.write(f'<row r="{row_idx}">'.encode("utf-8"))

                        for col_idx, value in enumerate(row, start=1):
                            cell_ref = f"{self.xlsx_col_name(col_idx)}{row_idx}"
                            text = self.clean_xml_text(value)

                            if text == "":
                                sheet_file.write(f'<c r="{cell_ref}"/>'.encode("utf-8"))
                                continue

                            preserve = ' xml:space="preserve"' if text.strip() != text else ""
                            escaped_text = xml_escape(text, quote=False)

                            cell_xml = (
                                f'<c r="{cell_ref}" t="inlineStr">'
                                f'<is><t{preserve}>{escaped_text}</t></is>'
                                f'</c>'
                            )
                            sheet_file.write(cell_xml.encode("utf-8"))

                        sheet_file.write(b'</row>')

            except Exception as e:
                error_text = xml_escape(f"Failed reading CSV: {csv_path} | {e}", quote=False)
                sheet_file.write(
                    f'<row r="1"><c r="A1" t="inlineStr"><is><t>{error_text}</t></is></c></row>'.encode("utf-8")
                )

            sheet_file.write(b'</sheetData></worksheet>')


    # ============================================================
    # CLOSE FILES
    # ============================================================
    def close_all_files(self):
        all_files = []

        for key in self.sensors:
            all_files += self.sensor_files.get(key, [])

        all_files += self.sync_files
        all_files += self.sync_diagnostic_files
        all_files += self.system_metrics_files
        all_files += (
            self.mission_log_files
            + self.mission_timeline_files
            + self.mission_readable_files
            + self.mission_events_files
            + self.mission_state_files
            + self.mission_waypoint_reached_files
            + self.mission_waypoints_files
            + self.mission_statustext_files
            + self.internet_status_files
            + self.internet_event_files
        )

        for file_obj in all_files:
            try:
                file_obj.flush()
                file_obj.close()
            except Exception:
                pass

    def destroy_node(self):
        if self.logging_active:
            self.stop_logging_session("node shutdown")
        else:
            self.close_all_files()

        try:
            os.sync()
        except AttributeError:
            pass

        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)

    node = SeanoLogger()
    executor = MultiThreadedExecutor(num_threads=8)
    executor.add_node(node)

    try:
        executor.spin()
    except KeyboardInterrupt:
        pass
    finally:
        executor.remove_node(node)
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()