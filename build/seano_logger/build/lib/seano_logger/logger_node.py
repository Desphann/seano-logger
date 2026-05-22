#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import glob
import math
import os
import socket
import time
from datetime import datetime

import psutil
import rclpy
from rclpy.node import Node
from rclpy.qos import qos_profile_sensor_data

from sensor_msgs.msg import NavSatFix, Imu, BatteryState
from std_msgs.msg import Float64MultiArray
from mavros_msgs.msg import (
    State,
    WaypointList,
    WaypointReached,
    VfrHud,
    StatusText,
)


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
        # RATES
        # ============================================================
        self.metrics_interval = 1.0
        self.mission_timeline_interval = 1.0
        self.internet_check_interval = 1.0

        # ============================================================
        # INTERNET MONITOR CONFIG
        # ============================================================
        # Internet umum: tidak perlu DNS.
        self.internet_probe_host = "1.1.1.1"
        self.internet_probe_port = 53
        self.internet_probe_timeout = 0.4

        # Cloud/MQTT SEANO.
        self.cloud_probe_host = "mqtt.seano.cloud"
        self.cloud_probe_port = 8883
        self.cloud_probe_timeout = 0.4

        # ============================================================
        # MISSION GATE STATE
        # ============================================================
        self.logging_active = False

        self.last_connected_state = False
        self.last_armed_state = False
        self.last_guided_state = False
        self.last_manual_input_state = False
        self.last_flight_mode = "UNKNOWN"
        self.last_flight_mode_class = "OTHER"
        self.last_system_status = 0

        self.prev_flight_mode = "UNKNOWN"
        self.prev_flight_mode_class = "OTHER"

        # ============================================================
        # SESSION STATE
        # ============================================================
        self.start_time_obj = None
        self.mission_start_monotonic = None
        self.local_timezone = time.tzname[0]
        self.mission_id = None
        self.base_paths = []

        # ============================================================
        # SENSOR LOGGER STATE
        # ============================================================
        self.files = {}
        self.csv_files = {}
        self.sample_count = {}
        self.detected_sensors = set()
        self.subscriptions_map = {}
        self.sensor_log_status = {}

        self.last_sensor_write_slot = {}
        self.last_sensor_write_wall_time = {}

        # ============================================================
        # SYSTEM METRICS STATE
        # ============================================================
        self.metrics_log_files = []
        self.metrics_csv_files = []
        self.bytes_written_since_last_metrics = 0
        self.last_metrics_time = time.time()
        self.jetson_temp_source = None

        # ============================================================
        # STORAGE RUNTIME STATE
        # ============================================================
        self.external_ready = False
        self.external_failed_runtime = False
        self.external_fail_reported = False

        # ============================================================
        # MISSION DATA CACHE
        # ============================================================
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

        # ============================================================
        # USER-FRIENDLY MISSION STATS
        # ============================================================
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

        # ============================================================
        # INTERNET MONITOR STATE
        # ============================================================
        self.internet_online = None
        self.cloud_online = None

        self.internet_lost_start_time = None
        self.cloud_lost_start_time = None

        self.internet_lost_count = 0
        self.cloud_lost_count = 0

        self.internet_total_down_sec = 0.0
        self.cloud_total_down_sec = 0.0

        self.internet_status_files = []
        self.internet_status_writers = []
        self.internet_event_files = []

        self.last_internet_write_slot = -1

        # ============================================================
        # MISSION FILE HANDLES
        # ============================================================
        self.mission_dirs = []
        self.mission_log_files = []
        self.mission_info_paths = []
        self.summary_paths = []

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

        # ============================================================
        # ROS SUBSCRIBERS
        # ============================================================
        self.state_sub = self.create_subscription(
            State,
            "/mavros/state",
            self.mavros_state_callback,
            10,
        )

        self.waypoints_sub = self.create_subscription(
            WaypointList,
            "/mavros/mission/waypoints",
            self.waypoints_callback,
            qos_profile_sensor_data,
        )

        self.waypoint_reached_sub = self.create_subscription(
            WaypointReached,
            "/mavros/mission/reached",
            self.waypoint_reached_callback,
            qos_profile_sensor_data,
        )

        self.vfr_hud_sub = self.create_subscription(
            VfrHud,
            "/mavros/vfr_hud",
            self.vfr_hud_callback,
            qos_profile_sensor_data,
        )

        self.gps_sub = self.create_subscription(
            NavSatFix,
            "/mavros/global_position/raw/fix",
            self.gps_callback,
            qos_profile_sensor_data,
        )

        self.battery_sub = self.create_subscription(
            BatteryState,
            "/battery/state",
            self.battery_callback,
            qos_profile_sensor_data,
        )

        self.statustext_sub = self.create_subscription(
            StatusText,
            "/mavros/statustext/recv",
            self.statustext_callback,
            qos_profile_sensor_data,
        )

        # ============================================================
        # TIMERS
        # ============================================================
        self.create_timer(2.0, self.detect_and_initialize_sensors)
        self.create_timer(self.metrics_interval, self.log_periodic_metrics)
        self.create_timer(self.mission_timeline_interval, self.log_mission_timeline)
        self.create_timer(self.internet_check_interval, self.monitor_internet_simple)
        self.create_timer(1.0, self.monitor_external_storage)

        psutil.cpu_percent(interval=None)

        self.get_logger().info("SEANO Logger standby")
        self.get_logger().info("Mission gate aktif: logging hanya saat /mavros/state armed=True")
        self.get_logger().info("Sensor, mission timeline, internet monitor, dan system metrics disampling 1 Hz")

    # ============================================================
    # TIME HELPERS
    # ============================================================
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

    def safe_value(self, value):
        if value is None:
            return ""
        if isinstance(value, float) and math.isnan(value):
            return ""
        return value

    # ============================================================
    # MODE CLASSIFICATION
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

    # ============================================================
    # MISSION GATE
    # ============================================================
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
            self.write_mission_event(
                "DISARM",
                f"mode={msg.mode}, armed={msg.armed}, connected={msg.connected}",
            )
            self.stop_logging_session(
                f"mode={msg.mode}, armed={msg.armed}, connected={msg.connected}"
            )

        if self.logging_active:
            if msg.mode != self.prev_flight_mode:
                self.write_mission_state_change(
                    self.prev_flight_mode,
                    msg.mode,
                    self.prev_flight_mode_class,
                    mode_class,
                )

                self.write_mission_event(
                    "MODE_CHANGE",
                    f"{self.prev_flight_mode} -> {msg.mode}",
                )

                if mode_class == "RETURN_HOME":
                    self.write_mission_event(
                        "RETURN_HOME_DETECTED",
                        f"mode={msg.mode}",
                    )

        self.prev_flight_mode = msg.mode
        self.prev_flight_mode_class = mode_class

    def start_logging_session(self, state_msg=None):
        if self.logging_active:
            return

        self.get_logger().info("ARMED detected -> preparing logger session")

        self.reset_session_state()

        self.start_time_obj = datetime.now()
        self.local_timezone = time.tzname[0]
        self.mission_start_monotonic = time.monotonic()

        year = self.start_time_obj.strftime("%Y")
        month = self.start_time_obj.strftime("%m")
        day = self.start_time_obj.strftime("%d")

        self.mission_id = self.start_time_obj.strftime(
            f"MISSION_START_%H-%M-%S_{self.local_timezone}"
        )

        if not self.prepare_base_paths(year, month, day):
            self.get_logger().fatal("Tidak ada path logging valid. Logger kembali standby.")
            return

        self.write_mission_start_info(state_msg)
        self.init_metrics_logger()
        self.init_mission_logger()

        self.logging_active = True
        self.last_metrics_time = time.time()
        self.bytes_written_since_last_metrics = 0
        psutil.cpu_percent(interval=None)

        if state_msg is not None:
            self.prev_flight_mode = state_msg.mode
            self.prev_flight_mode_class = self.classify_mode(state_msg.mode)

        self.write_mission_event(
            "ARM",
            f"mode={self.last_flight_mode}, connected={self.last_connected_state}",
        )

        if self.last_waypoints_msg is not None:
            self.dump_waypoints(self.last_waypoints_msg)

        self.detect_and_initialize_sensors()

        for path in self.base_paths:
            self.get_logger().info(f"Mission folder: {path}")

        self.get_logger().info(
            f"LOGGER ACTIVE | mode={self.last_flight_mode}, "
            f"armed={self.last_armed_state}, connected={self.last_connected_state}"
        )

    def reset_session_state(self):
        self.external_ready = False
        self.external_failed_runtime = False
        self.external_fail_reported = False
        self.sensor_log_status = {}

        self.base_paths = []
        self.files = {}
        self.csv_files = {}
        self.sample_count = {}
        self.detected_sensors = set()
        self.subscriptions_map = {}

        self.metrics_log_files = []
        self.metrics_csv_files = []
        self.bytes_written_since_last_metrics = 0
        self.last_metrics_time = time.time()

        self.last_sensor_write_slot = {}
        self.last_sensor_write_wall_time = {}

        self.reset_mission_files()
        self.reset_user_friendly_mission_stats()

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
                    self.get_logger().error(
                        f"Gagal menyiapkan external logging: {external_base_path} | {e}"
                    )
            else:
                self.get_logger().warning(
                    f"SSD external belum siap / tidak writable: {self.external_mount_point}"
                )

            if self.require_external_on_mission and not self.external_ready:
                self.get_logger().fatal(
                    "Armed terdeteksi, tetapi SSD external belum siap. Logger tetap standby."
                )
                return False

        if self.enable_local_logging:
            local_base_path = os.path.join(
                self.local_mount_point,
                year,
                month,
                day,
                self.mission_id,
            )

            try:
                os.makedirs(local_base_path, exist_ok=True)

                if not self.test_write_access(local_base_path):
                    raise RuntimeError("Local path not writable")

                self.base_paths.append(local_base_path)
                self.get_logger().info(f"Local logging ready: {local_base_path}")

            except Exception as e:
                self.get_logger().error(
                    f"Gagal membuat folder local logging: {local_base_path} | {e}"
                )

        return len(self.base_paths) > 0

    def stop_logging_session(self, reason="vehicle disarmed"):
        if not self.logging_active:
            return

        self.logging_active = False

        self.get_logger().info(f"DISARM detected -> closing logger session | {reason}")

        self.write_mission_end_info(reason)
        self.destroy_sensor_subscriptions()
        self.close_all_files()

        try:
            os.sync()
        except AttributeError:
            pass

        self.base_paths = []
        self.files = {}
        self.csv_files = {}
        self.sample_count = {}
        self.detected_sensors = set()
        self.subscriptions_map = {}

        self.metrics_log_files = []
        self.metrics_csv_files = []

        self.reset_mission_files()

        self.bytes_written_since_last_metrics = 0
        self.sensor_log_status = {}
        self.external_ready = False
        self.mission_start_monotonic = None

        self.last_sensor_write_slot = {}
        self.last_sensor_write_wall_time = {}

        self.get_logger().info("SEANO Logger kembali standby, menunggu armed berikutnya")

    # ============================================================
    # STORAGE HELPERS
    # ============================================================
    def is_path_writable(self, path):
        return os.path.exists(path) and os.access(path, os.W_OK)

    def test_write_access(self, path):
        test_file = os.path.join(path, ".seano_write_test")

        try:
            with open(test_file, "w") as f:
                f.write("ok")

            os.remove(test_file)
            return True

        except Exception:
            return False

    def monitor_external_storage(self):
        if not self.logging_active:
            return

        if not self.enable_external_logging or not self.external_ready:
            return

        if not self.is_path_writable(self.external_mount_point):
            if not self.external_fail_reported:
                self.get_logger().fatal(
                    f"SSD external terputus / tidak writable lagi: {self.external_mount_point}"
                )
                self.get_logger().fatal(
                    "Logging ke SSD bermasalah. Local mungkin masih aktif, tapi external gagal."
                )

                self.external_fail_reported = True
                self.external_failed_runtime = True

    # ============================================================
    # INTERNET MONITOR - USER FRIENDLY
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

        line = (
            f"{self.get_local_timestamp()} | "
            f"t={self.get_mission_elapsed_sec():.1f}s | "
            f"{text}\n"
        )

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

        internet_down = self.get_current_total_internet_down_sec()
        cloud_down = self.get_current_total_cloud_down_sec()

        row = [
            self.get_local_timestamp(),
            f"{self.get_mission_elapsed_sec():.1f}",
            self.internet_text(self.internet_online),
            self.internet_text(self.cloud_online),
            self.internet_lost_count,
            self.cloud_lost_count,
            f"{internet_down:.1f}",
            f"{cloud_down:.1f}",
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

        internet_now = self.tcp_probe(
            self.internet_probe_host,
            self.internet_probe_port,
            self.internet_probe_timeout,
        )

        cloud_now = self.tcp_probe(
            self.cloud_probe_host,
            self.cloud_probe_port,
            self.cloud_probe_timeout,
        )

        now = time.time()
        note = "OK"

        # ========================================================
        # INTERNET UMUM
        # ========================================================
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

        if self.internet_online is False and self.internet_lost_start_time is not None:
            ongoing_down = now - self.internet_lost_start_time
            note = f"INTERNET MASIH MATI {ongoing_down:.1f} detik"

        # ========================================================
        # CLOUD MQTT
        # ========================================================
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

        if self.cloud_online is False and self.cloud_lost_start_time is not None:
            ongoing_down = now - self.cloud_lost_start_time
            if note == "OK":
                note = f"MQTT CLOUD MASIH MATI {ongoing_down:.1f} detik"

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
    # JETSON TEMP
    # ============================================================
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
    # ROOT MISSION INFO
    # ============================================================
    def write_mission_start_info(self, state_msg=None):
        for base_path in self.base_paths:
            mission_info_path = os.path.join(base_path, "mission_info.txt")

            with open(mission_info_path, "w") as f:
                f.write(f"Start Time: {self.start_time_obj}\n")
                f.write(f"Timezone: {self.local_timezone}\n")
                f.write("Platform: SEANO USV\n")
                f.write("Logger Mode: Armed gated\n")
                f.write("Sensor Log Rate Limit: max 1 Hz per sensor\n")
                f.write("Mission Timeline Rate: 1 Hz\n")
                f.write("Internet Monitor Rate: 1 Hz\n")
                f.write("System Metrics Rate: 1 Hz\n")
                f.write("Mission Gate: /mavros/state armed=True\n")

                if state_msg is not None:
                    f.write(f"Start MAVROS Connected: {state_msg.connected}\n")
                    f.write(f"Start MAVROS Armed: {state_msg.armed}\n")
                    f.write(f"Start MAVROS Mode: {state_msg.mode}\n")

    def write_mission_end_info(self, reason):
        end_time_obj = datetime.now()
        elapsed = self.get_mission_elapsed_sec()

        self.write_mission_summary(reason, end_time_obj, elapsed)

        for base_path in self.base_paths:
            mission_info_path = os.path.join(base_path, "mission_info.txt")

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
    # SYSTEM METRICS
    # ============================================================
    def init_metrics_logger(self):
        self.metrics_log_files = []
        self.metrics_csv_files = []

        for base_path in self.base_paths:
            metrics_log_path = os.path.join(base_path, "system_metrics.log")
            metrics_csv_path = os.path.join(base_path, "system_metrics.csv")

            metrics_log_file = open(metrics_log_path, "w")
            metrics_csv_file = open(metrics_csv_path, "w")

            metrics_log_file.write(
                "[System]\n"
                "Platform=SEANO USV\n"
                f"Start_Time={self.start_time_obj.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"Timezone={self.local_timezone}\n\n"
                "[Sensor]\n"
                "Name=System Metrics\n\n"
                "[Columns]\n"
                f"Timestamp({self.local_timezone})\t"
                "WriteSpeed(Bps)\tCPU(%)\tRAM(%)\tJetsonTemp(C)\t"
                "GPS_Hz\tIMU_Hz\tCTD_Hz\tADCP_Hz\tBATTERY_Hz\tSBES_Hz\tMISSION_Hz\n\n"
                "[Data]\n"
            )

            metrics_csv_file.write(
                "timestamp,write_speed_Bps,cpu_percent,ram_percent,jetson_temp_c,"
                "gps_hz,imu_hz,ctd_hz,adcp_hz,battery_hz,sbes_hz,mission_hz\n"
            )

            self.metrics_log_files.append(metrics_log_file)
            self.metrics_csv_files.append(metrics_csv_file)

    def get_sensor_rate(self, key):
        last_write_time = self.last_sensor_write_wall_time.get(key, 0.0)

        if last_write_time <= 0.0:
            return 0.0

        age = time.time() - last_write_time
        return 1.0 if age <= 1.5 else 0.0

    def get_mission_timeline_rate(self):
        if self.last_mission_timeline_write_time <= 0.0:
            return 0.0

        age = time.time() - self.last_mission_timeline_write_time
        return 1.0 if age <= 1.5 else 0.0

    def log_periodic_metrics(self):
        if not self.logging_active or not self.metrics_log_files:
            self.bytes_written_since_last_metrics = 0
            self.last_metrics_time = time.time()
            return

        now = time.time()
        elapsed = now - self.last_metrics_time if self.last_metrics_time else self.metrics_interval
        t = self.get_local_timestamp()

        write_speed = self.bytes_written_since_last_metrics / elapsed if elapsed > 0 else 0.0
        cpu_usage = psutil.cpu_percent(interval=None)
        ram_usage = psutil.virtual_memory().percent

        jetson_temp_c = self.read_jetson_temperature()
        jetson_temp_str = f"{jetson_temp_c:.2f}" if jetson_temp_c is not None else "nan"

        gps_hz = self.get_sensor_rate("gps")
        imu_hz = self.get_sensor_rate("imu")
        ctd_hz = self.get_sensor_rate("ctd")
        adcp_hz = self.get_sensor_rate("adcp")
        battery_hz = self.get_sensor_rate("battery")
        sbes_hz = self.get_sensor_rate("sbes")
        mission_hz = self.get_mission_timeline_rate()

        log_line = (
            f"{t}\t{write_speed:.2f}\t{cpu_usage:.2f}\t{ram_usage:.2f}\t"
            f"{jetson_temp_str}\t{gps_hz:.2f}\t{imu_hz:.2f}\t{ctd_hz:.2f}\t"
            f"{adcp_hz:.2f}\t{battery_hz:.2f}\t{sbes_hz:.2f}\t{mission_hz:.2f}\n"
        )

        csv_line = (
            f"{t},{write_speed:.2f},{cpu_usage:.2f},{ram_usage:.2f},"
            f"{jetson_temp_str},{gps_hz:.2f},{imu_hz:.2f},{ctd_hz:.2f},"
            f"{adcp_hz:.2f},{battery_hz:.2f},{sbes_hz:.2f},{mission_hz:.2f}\n"
        )

        for i, metrics_log_file in enumerate(self.metrics_log_files):
            try:
                metrics_log_file.write(log_line)
                metrics_log_file.flush()

                self.metrics_csv_files[i].write(csv_line)
                self.metrics_csv_files[i].flush()
            except Exception as e:
                self.get_logger().error(f"Metrics logging failed on target {i}: {e}")

        self.bytes_written_since_last_metrics = 0
        self.last_metrics_time = now

    # ============================================================
    # SENSOR DETECTION
    # ============================================================
    def detect_and_initialize_sensors(self):
        if not self.logging_active:
            return

        topics = dict(self.get_topic_names_and_types())

        if "/mavros/global_position/raw/fix" in topics and "gps" not in self.detected_sensors:
            self.init_sensor(
                "gps",
                "GPS",
                f"Timestamp({self.local_timezone})\tLatitude\tLongitude\tAltitude",
                "timestamp,latitude,longitude,altitude",
            )
            self.detected_sensors.add("gps")
            self.get_logger().info("GPS detected")

        if "/mavros/imu/data" in topics and "imu" not in self.detected_sensors:
            self.init_sensor(
                "imu",
                "IMU",
                f"Timestamp({self.local_timezone})\tAccX\tAccY\tAccZ",
                "timestamp,acc_x,acc_y,acc_z",
            )
            self.subscriptions_map["imu"] = self.create_subscription(
                Imu,
                "/mavros/imu/data",
                self.imu_callback,
                qos_profile_sensor_data,
            )
            self.detected_sensors.add("imu")
            self.get_logger().info("IMU detected")

        if "/ctd/data" in topics and "ctd" not in self.detected_sensors:
            self.init_sensor(
                "ctd",
                "CTD",
                f"Timestamp({self.local_timezone})\tDepth\tTemp\tCond\tSalinity\tDensity\tSoundVel",
                "timestamp,depth,temp,cond,salinity,density,soundvel",
            )
            self.subscriptions_map["ctd"] = self.create_subscription(
                Float64MultiArray,
                "/ctd/data",
                self.ctd_callback,
                50,
            )
            self.detected_sensors.add("ctd")
            self.get_logger().info("CTD detected")

        if "/adcp/data" in topics and "adcp" not in self.detected_sensors:
            self.init_sensor(
                "adcp",
                "ADCP",
                (
                    f"Timestamp({self.local_timezone})\tNumCells\tNumBeams\tCellSizeM\t"
                    "BlankingDistanceM\tHeadingDeg\tPitchDeg\tRollDeg\tTemperatureC\t"
                    "SalinityPSU\tPressureDbar\tVelocityProfile"
                ),
                (
                    "timestamp,num_cells,num_beams,cell_size_m,blanking_distance_m,"
                    "heading_deg,pitch_deg,roll_deg,temperature_c,salinity_psu,"
                    "pressure_dbar,velocity_profile"
                ),
            )
            self.subscriptions_map["adcp"] = self.create_subscription(
                Float64MultiArray,
                "/adcp/data",
                self.adcp_callback,
                10,
            )
            self.detected_sensors.add("adcp")
            self.get_logger().info("ADCP detected")

        if "/battery/state" in topics and "battery" not in self.detected_sensors:
            self.init_sensor(
                "battery",
                "Battery",
                f"Timestamp({self.local_timezone})\tVoltage(V)\tCurrent(A)\tPercentage(%)",
                "timestamp,voltage_v,current_a,percentage_percent",
            )
            self.detected_sensors.add("battery")
            self.get_logger().info("Battery detected from /battery/state")

        if "/sbes/data" in topics and "sbes" not in self.detected_sensors:
            self.init_sensor(
                "sbes",
                "SBES",
                f"Timestamp({self.local_timezone})\tDepth\tWaterTemp\tQualityFlag",
                "timestamp,depth,water_temp,quality_flag",
            )
            self.subscriptions_map["sbes"] = self.create_subscription(
                Float64MultiArray,
                "/sbes/data",
                self.sbes_callback,
                10,
            )
            self.detected_sensors.add("sbes")
            self.get_logger().info("SBES detected")

    def init_sensor(self, key, name, log_columns, csv_columns):
        if not self.logging_active or not self.base_paths:
            return

        self.files[key] = []
        self.csv_files[key] = []
        self.sample_count[key] = 0
        self.sensor_log_status[key] = False
        self.last_sensor_write_slot[key] = -1
        self.last_sensor_write_wall_time[key] = 0.0

        for base_path in self.base_paths:
            log_path = os.path.join(base_path, f"{key}.log")
            csv_path = os.path.join(base_path, f"{key}.csv")

            log_file = open(log_path, "w")
            csv_file = open(csv_path, "w")

            log_file.write(
                "[System]\n"
                "Platform=SEANO USV\n"
                f"Start_Time={self.start_time_obj.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"Timezone={self.local_timezone}\n"
                "RateLimit=max 1 Hz\n"
                "Throttle=realtime second slot\n\n"
                "[Sensor]\n"
                f"Name={name}\n\n"
                "[Columns]\n"
                f"{log_columns}\n\n"
                "[Data]\n"
            )

            csv_file.write(csv_columns + "\n")

            self.files[key].append(log_file)
            self.csv_files[key].append(csv_file)

    def write_sensor_data(self, key, log_line, csv_line):
        if not self.logging_active:
            return

        if key not in self.files:
            return

        current_slot = int(time.time())
        last_slot = self.last_sensor_write_slot.get(key, -1)

        if last_slot == current_slot:
            return

        self.last_sensor_write_slot[key] = current_slot
        self.last_sensor_write_wall_time[key] = time.time()

        success_count = 0

        for idx, log_file in enumerate(self.files[key]):
            try:
                log_file.write(log_line)
                log_file.flush()

                self.csv_files[key][idx].write(csv_line)
                self.csv_files[key][idx].flush()

                success_count += 1

            except Exception as e:
                self.get_logger().error(f"{key.upper()} logging failed on target {idx}: {e}")

        if success_count > 0:
            self.bytes_written_since_last_metrics += len(log_line.encode("utf-8"))
            self.bytes_written_since_last_metrics += len(csv_line.encode("utf-8"))
            self.sample_count[key] = self.sample_count.get(key, 0) + 1

            if not self.sensor_log_status.get(key, False):
                self.get_logger().info(f"{key.upper()} logging active")
                self.sensor_log_status[key] = True

        else:
            if self.sensor_log_status.get(key, False):
                self.get_logger().error(f"{key.upper()} logging failed on all targets")
                self.sensor_log_status[key] = False

    # ============================================================
    # SENSOR CALLBACKS
    # ============================================================
    def gps_callback(self, msg):
        self.last_latitude = msg.latitude
        self.last_longitude = msg.longitude
        self.last_gps_altitude = msg.altitude
        self.last_gps_status = msg.status.status
        self.last_gps_service = msg.status.service

        t = self.get_local_timestamp()

        log_line = f"{t}\t{msg.latitude}\t{msg.longitude}\t{msg.altitude}\n"
        csv_line = f"{t},{msg.latitude},{msg.longitude},{msg.altitude}\n"

        self.write_sensor_data("gps", log_line, csv_line)

    def imu_callback(self, msg):
        t = self.get_local_timestamp()

        log_line = (
            f"{t}\t"
            f"{msg.linear_acceleration.x}\t"
            f"{msg.linear_acceleration.y}\t"
            f"{msg.linear_acceleration.z}\n"
        )

        csv_line = (
            f"{t},"
            f"{msg.linear_acceleration.x},"
            f"{msg.linear_acceleration.y},"
            f"{msg.linear_acceleration.z}\n"
        )

        self.write_sensor_data("imu", log_line, csv_line)

    def ctd_callback(self, msg):
        t = self.get_local_timestamp()
        data = list(msg.data)

        if len(data) >= 6:
            depth, temp, cond, salinity, density, soundvel = data[:6]
            log_line = f"{t}\t{depth}\t{temp}\t{cond}\t{salinity}\t{density}\t{soundvel}\n"
            csv_line = f"{t},{depth},{temp},{cond},{salinity},{density},{soundvel}\n"
        else:
            log_line = f"{t}\t" + "\t".join(map(str, data)) + "\n"
            csv_line = f"{t}," + ",".join(map(str, data)) + "\n"

        self.write_sensor_data("ctd", log_line, csv_line)

    def adcp_callback(self, msg):
        t = self.get_local_timestamp()
        data = list(msg.data)

        if len(data) >= 10:
            num_cells = int(data[0])
            num_beams = int(data[1])
            cell_size_m = data[2]
            blanking_distance_m = data[3]
            heading_deg = data[4]
            pitch_deg = data[5]
            roll_deg = data[6]
            temperature_c = data[7]
            salinity_psu = data[8]
            pressure_dbar = data[9]
            velocity_profile = ";".join(map(str, data[10:]))

            log_line = (
                f"{t}\t{num_cells}\t{num_beams}\t{cell_size_m}\t"
                f"{blanking_distance_m}\t{heading_deg}\t{pitch_deg}\t"
                f"{roll_deg}\t{temperature_c}\t{salinity_psu}\t"
                f"{pressure_dbar}\t{velocity_profile}\n"
            )

            csv_line = (
                f"{t},{num_cells},{num_beams},{cell_size_m},"
                f"{blanking_distance_m},{heading_deg},{pitch_deg},"
                f"{roll_deg},{temperature_c},{salinity_psu},"
                f"{pressure_dbar},\"{velocity_profile}\"\n"
            )

        else:
            log_line = f"{t}\t" + "\t".join(map(str, data)) + "\n"
            csv_line = f"{t}," + ",".join(map(str, data)) + "\n"

        self.write_sensor_data("adcp", log_line, csv_line)

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

        t = self.get_local_timestamp()

        log_line = f"{t}\t{voltage:.3f}\t{current:.3f}\t{percentage_percent:.2f}\n"
        csv_line = f"{t},{voltage:.3f},{current:.3f},{percentage_percent:.2f}\n"

        self.write_sensor_data("battery", log_line, csv_line)

    def sbes_callback(self, msg):
        t = self.get_local_timestamp()
        data = list(msg.data)

        if len(data) >= 3:
            depth, water_temp, quality_flag = data[:3]
            log_line = f"{t}\t{depth}\t{water_temp}\t{quality_flag}\n"
            csv_line = f"{t},{depth},{water_temp},{quality_flag}\n"
        else:
            log_line = f"{t}\t" + "\t".join(map(str, data)) + "\n"
            csv_line = f"{t}," + ",".join(map(str, data)) + "\n"

        self.write_sensor_data("sbes", log_line, csv_line)

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

        self.write_mission_event(
            "WAYPOINT_LIST_UPDATED",
            f"count={len(msg.waypoints)}, current_seq={msg.current_seq}",
        )

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

        if (
            "RTL" in text_upper
            or "RTH" in text_upper
            or "RETURN" in text_upper
            or "FAILSAFE" in text_upper
        ):
            self.write_mission_event("AUTOPILOT_STATUS_ALERT", text, ros_time=ros_time)

    # ============================================================
    # MISSION FILE SETUP
    # ============================================================
    def reset_mission_files(self):
        self.mission_dirs = []
        self.mission_log_files = []
        self.mission_info_paths = []
        self.summary_paths = []

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
        self.reset_mission_files()

        for base_path in self.base_paths:
            mission_dir = os.path.join(base_path, "mission")
            os.makedirs(mission_dir, exist_ok=True)

            self.mission_dirs.append(mission_dir)

            readme_path = os.path.join(mission_dir, "00_READ_ME_FIRST.txt")
            summary_path = os.path.join(mission_dir, "mission_summary.txt")
            info_path = os.path.join(mission_dir, "mission_info.txt")
            log_path = os.path.join(mission_dir, "mission_events_readable.log")

            self.summary_paths.append(summary_path)
            self.mission_info_paths.append(info_path)

            with open(readme_path, "w") as f:
                f.write("SEANO Mission Log - Quick Guide\n")
                f.write("================================\n\n")
                f.write("Open these first:\n")
                f.write("1. mission_summary.txt          : Ringkasan akhir misi.\n")
                f.write("2. mission_readable.csv         : Timeline 1 Hz yang mudah dibaca.\n")
                f.write("3. mission_events_readable.log  : Kronologi event penting.\n")
                f.write("4. internet_status.csv          : Status internet 1 Hz.\n")
                f.write("5. internet_events.log          : Kapan internet mati/nyala.\n\n")
                f.write("Detailed files for analysis:\n")
                f.write("- mission_timeline.csv\n")
                f.write("- mission_events.csv\n")
                f.write("- mission_state_changes.csv\n")
                f.write("- mission_waypoint_reached.csv\n")
                f.write("- mission_waypoints.csv\n")
                f.write("- mission_statustext.csv\n")

            with open(info_path, "w") as f:
                f.write(f"Mission Logger Start: {self.get_local_timestamp()}\n")
                f.write(f"Mission ID: {self.mission_id}\n")
                f.write(f"Mission Folder: {base_path}\n")
                f.write(f"Mission Log Dir: {mission_dir}\n")
                f.write("Mission Timeline Rate: 1 Hz\n")
                f.write("Internet Monitor Rate: 1 Hz\n")
                f.write(f"Internet Probe: {self.internet_probe_host}:{self.internet_probe_port}\n")
                f.write(f"MQTT Cloud Probe: {self.cloud_probe_host}:{self.cloud_probe_port}\n")
                f.write("Return-home keywords: RTL, RTH, RETURN\n")
                f.write("Failsafe keywords: FAILSAFE\n")

            mission_log_file = open(log_path, "w", buffering=1)
            mission_log_file.write("SEANO Mission Events\n")
            mission_log_file.write("====================\n")
            self.mission_log_files.append(mission_log_file)

            # ========================================================
            # INTERNET FILES - EASY TO READ
            # ========================================================
            internet_status_path = os.path.join(mission_dir, "internet_status.csv")
            internet_events_path = os.path.join(mission_dir, "internet_events.log")

            internet_status_file = open(internet_status_path, "w", newline="", buffering=1)
            internet_status_writer = csv.writer(internet_status_file)

            internet_status_writer.writerow(
                [
                    "time",
                    "elapsed_sec",
                    "internet",
                    "cloud_mqtt",
                    "internet_lost_count",
                    "cloud_lost_count",
                    "internet_total_down_sec",
                    "cloud_total_down_sec",
                    "note",
                ]
            )

            internet_event_file = open(internet_events_path, "w", buffering=1)
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
    # MISSION WRITE HELPERS
    # ============================================================
    def write_mission_event(self, event_type, detail, ros_time=None):
        if not self.logging_active:
            return

        self.mission_event_count += 1

        event_upper = str(event_type).upper()
        detail_upper = str(detail).upper()

        if (
            "RETURN" in event_upper
            or "RTL" in detail_upper
            or "RTH" in detail_upper
            or "RETURN" in detail_upper
        ):
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
            parts.append(
                ":".join(
                    [
                        str(wp.frame),
                        str(wp.command),
                        f"{wp.x_lat:.8f}",
                        f"{wp.y_long:.8f}",
                        f"{wp.z_alt:.3f}",
                    ]
                )
            )

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

        step = self.haversine_m(
            self.mission_last_distance_lat,
            self.mission_last_distance_lon,
            lat,
            lon,
        )

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

        a = (
            math.sin(dp / 2.0) ** 2
            + math.cos(p1) * math.cos(p2) * math.sin(dl / 2.0) ** 2
        )

        c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))

        return radius_m * c

    def write_mission_summary(self, reason, end_time_obj, elapsed):
        avg_speed = 0.0

        if self.mission_speed_sample_count > 0:
            avg_speed = self.mission_speed_sum_mps / self.mission_speed_sample_count

        self.mission_end_battery_percent = self.last_battery_percent

        internet_total_down = self.get_current_total_internet_down_sec()
        cloud_total_down = self.get_current_total_cloud_down_sec()

        for path in self.summary_paths:
            try:
                with open(path, "w") as f:
                    f.write("SEANO Mission Summary\n")
                    f.write("=====================\n\n")

                    f.write(f"Mission ID              : {self.mission_id}\n")
                    f.write(f"Start time              : {self.start_time_obj}\n")
                    f.write(f"End time                : {end_time_obj}\n")
                    f.write(f"Duration                : {elapsed:.1f} sec ({elapsed / 60.0:.2f} min)\n")
                    f.write(f"Stop reason             : {reason}\n\n")

                    f.write("Final state\n")
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
                    f.write("1. mission_readable.csv\n")
                    f.write("2. mission_events_readable.log\n")
                    f.write("3. internet_status.csv\n")
                    f.write("4. internet_events.log\n")
                    f.write("5. mission_timeline.csv\n")

            except Exception as e:
                self.get_logger().error(f"Failed writing mission summary: {e}")

    # ============================================================
    # SHUTDOWN HELPERS
    # ============================================================
    def destroy_sensor_subscriptions(self):
        for key, subscription in list(self.subscriptions_map.items()):
            try:
                self.destroy_subscription(subscription)
            except Exception as e:
                self.get_logger().warning(f"Failed to destroy {key} subscription: {e}")

        self.subscriptions_map = {}

    def close_all_files(self):
        for key in list(self.files.keys()):
            for log_file in self.files.get(key, []):
                try:
                    log_file.close()
                except Exception:
                    pass

            for csv_file in self.csv_files.get(key, []):
                try:
                    csv_file.close()
                except Exception:
                    pass

        for metrics_log_file in self.metrics_log_files:
            try:
                metrics_log_file.close()
            except Exception:
                pass

        for metrics_csv_file in self.metrics_csv_files:
            try:
                metrics_csv_file.close()
            except Exception:
                pass

        all_mission_files = (
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

        for file_obj in all_mission_files:
            try:
                file_obj.flush()
                file_obj.close()
            except Exception:
                pass

    def destroy_node(self):
        if self.logging_active:
            self.stop_logging_session("node shutdown")
        else:
            self.destroy_sensor_subscriptions()
            self.close_all_files()

        try:
            os.sync()
        except AttributeError:
            pass

        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)

    node = SeanoLogger()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass

    node.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()