import glob
import math
import os
import time
from datetime import datetime

import psutil
import rclpy
from rclpy.node import Node
from rclpy.qos import qos_profile_sensor_data

from sensor_msgs.msg import NavSatFix, Imu, BatteryState
from std_msgs.msg import Float64MultiArray
from mavros_msgs.msg import State


class SeanoLogger(Node):
    def __init__(self):
        super().__init__('logger_node')

        # =========================================================
        # Konfigurasi penyimpanan
        # =========================================================
        self.external_mount_point = "/mnt/seano/SEANO_SSD"
        # self.external_mount_point = "/media/raihan/SEANO"
        self.local_mount_point = os.path.expanduser("~/Documents/SEANO_logs")

        self.enable_external_logging = True
        self.enable_local_logging = True

        # Jika True, saat armed logger tidak akan membuat sesi
        # kalau SSD external belum siap. Jika False, logger tetap fallback ke local.
        self.require_external_on_mission = False

        # =========================================================
        # Rate limit sensor logging
        # Semua sensor dibatasi maksimal 1 data per detik realtime.
        # Jadi lebih sinkron daripada throttle berdasarkan waktu callback.
        # =========================================================
        self.last_sensor_write_slot = {}
        self.last_sensor_write_wall_time = {}

        # Interval logging metrics sistem tetap 1 Hz
        self.metrics_interval = 1.0

        # =========================================================
        # Mission gate
        # Logger hidup standby sejak Jetson boot, tapi hanya menulis
        # saat /mavros/state menunjukkan armed=True.
        # =========================================================
        self.logging_active = False
        self.last_flight_mode = "UNKNOWN"
        self.last_armed_state = False
        self.last_connected_state = False

        self.state_sub = self.create_subscription(
            State,
            "/mavros/state",
            self.mavros_state_callback,
            10
        )

        # =========================================================
        # State logger
        # =========================================================
        self.external_ready = False
        self.external_failed_runtime = False
        self.external_fail_reported = False
        self.sensor_log_status = {}

        self.jetson_temp_source = None

        # Dibuat ulang setiap armed start
        self.start_time_obj = None
        self.local_timezone = time.tzname[0]
        self.mission_id = None

        # Kosong saat standby
        self.base_paths = []

        # File dan subscription sensor aktif
        self.files = {}
        self.csv_files = {}
        self.sample_count = {}
        self.detected_sensors = set()
        self.subscriptions_map = {}

        # Metrics system
        self.metrics_log_files = []
        self.metrics_csv_files = []
        self.last_sample_count = {}
        self.last_metrics_time = time.time()
        self.bytes_written_since_last_metrics = 0

        # Timer tetap hidup sejak boot,
        # tapi fungsi di dalamnya return kalau logging_active masih False.
        self.create_timer(2.0, self.detect_and_initialize_sensors)
        self.create_timer(self.metrics_interval, self.log_periodic_metrics)
        self.create_timer(1.0, self.monitor_external_storage)

        # Prime psutil CPU
        psutil.cpu_percent(interval=None)

        self.get_logger().info("SEANO Logger standby")
        self.get_logger().info(
            "Mission gate aktif: logging hanya saat /mavros/state armed=True"
        )
        self.get_logger().info(
            "Sensor logging rate limit: max 1 Hz per sensor, synced by realtime second slot"
        )

    # =========================================================
    # Mission gate
    # =========================================================
    def mavros_state_callback(self, msg):
        self.last_connected_state = msg.connected
        self.last_armed_state = msg.armed
        self.last_flight_mode = msg.mode

        # Logger mulai saat kendaraan ARMED.
        # Mode tidak dicek, jadi MANUAL/GUIDED/AUTO/RTL tetap logging
        # selama armed=True.
        mission_should_log = msg.armed

        if mission_should_log and not self.logging_active:
            self.start_logging_session(msg)

        elif not mission_should_log and self.logging_active:
            reason = (
                f"mode={msg.mode}, "
                f"armed={msg.armed}, "
                f"connected={msg.connected}"
            )
            self.stop_logging_session(reason)

    def start_logging_session(self, state_msg=None):
        if self.logging_active:
            return

        self.get_logger().info("ARMED detected -> preparing logger session")

        # Reset state sesi baru
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
        self.last_sample_count = {}

        self.bytes_written_since_last_metrics = 0
        self.last_metrics_time = time.time()

        # Reset throttle setiap sesi start
        self.last_sensor_write_slot = {}
        self.last_sensor_write_wall_time = {}

        self.start_time_obj = datetime.now()
        self.local_timezone = time.tzname[0]

        year = self.start_time_obj.strftime("%Y")
        month = self.start_time_obj.strftime("%m")
        day = self.start_time_obj.strftime("%d")

        self.mission_id = self.start_time_obj.strftime(
            f"MISSION_START_%H-%M-%S_{self.local_timezone}"
        )

        # =========================================================
        # Siapkan external SSD saat armed
        # =========================================================
        if self.enable_external_logging:
            if self.is_path_writable(self.external_mount_point):
                external_base_path = os.path.join(
                    self.external_mount_point,
                    "SEANO_MISSIONS",
                    year,
                    month,
                    day,
                    self.mission_id
                )

                try:
                    os.makedirs(external_base_path, exist_ok=True)

                    if not self.test_write_access(external_base_path):
                        raise RuntimeError("External SSD detected but not writable")

                    self.base_paths.append(external_base_path)
                    self.external_ready = True

                    self.get_logger().info(
                        f"External logging ready: {external_base_path}"
                    )

                except Exception as e:
                    self.external_ready = False
                    self.external_failed_runtime = True

                    self.get_logger().error(
                        f"Gagal menyiapkan external logging: "
                        f"{external_base_path} | {e}"
                    )
            else:
                self.get_logger().warning(
                    f"SSD external belum siap / tidak writable: "
                    f"{self.external_mount_point}"
                )

            if self.require_external_on_mission and not self.external_ready:
                self.get_logger().fatal(
                    "Armed terdeteksi, tetapi SSD external belum siap. "
                    "Logger tetap standby dan tidak membuat file mission."
                )
                return

        # =========================================================
        # Siapkan local logging
        # =========================================================
        if self.enable_local_logging:
            local_base_path = os.path.join(
                self.local_mount_point,
                year,
                month,
                day,
                self.mission_id
            )

            try:
                os.makedirs(local_base_path, exist_ok=True)

                if not self.test_write_access(local_base_path):
                    raise RuntimeError("Local path not writable")

                self.base_paths.append(local_base_path)

                self.get_logger().info(
                    f"Local logging ready: {local_base_path}"
                )

            except Exception as e:
                self.get_logger().error(
                    f"Gagal membuat folder local logging: "
                    f"{local_base_path} | {e}"
                )

        if not self.base_paths:
            self.get_logger().fatal(
                "Tidak ada path logging yang valid. Logger kembali standby."
            )
            return

        self.write_mission_start_info(state_msg)
        self.init_metrics_logger()

        # Gate diaktifkan setelah file dan folder siap
        self.logging_active = True
        self.last_metrics_time = time.time()
        self.bytes_written_since_last_metrics = 0
        psutil.cpu_percent(interval=None)

        # Deteksi sensor langsung saat armed
        self.detect_and_initialize_sensors()

        for path in self.base_paths:
            self.get_logger().info(f"Mission folder: {path}")

        self.get_logger().info(
            f"LOGGER ACTIVE | "
            f"mode={self.last_flight_mode}, "
            f"armed={self.last_armed_state}, "
            f"connected={self.last_connected_state}"
        )

    def stop_logging_session(self, reason="vehicle disarmed"):
        if not self.logging_active:
            return

        # Matikan gate dulu supaya callback sensor yang masih antre
        # tidak menulis lagi ke file.
        self.logging_active = False

        self.get_logger().info(
            f"DISARM detected -> closing logger session | {reason}"
        )

        self.write_mission_end_info(reason)
        self.destroy_sensor_subscriptions()
        self.close_all_files()

        try:
            os.sync()
        except AttributeError:
            pass

        # Reset state sesi, tapi node, timer, dan /mavros/state subscriber tetap hidup
        self.base_paths = []
        self.files = {}
        self.csv_files = {}
        self.sample_count = {}
        self.detected_sensors = set()
        self.subscriptions_map = {}

        self.metrics_log_files = []
        self.metrics_csv_files = []
        self.last_sample_count = {}

        self.bytes_written_since_last_metrics = 0
        self.sensor_log_status = {}
        self.external_ready = False

        # Reset throttle saat kembali standby
        self.last_sensor_write_slot = {}
        self.last_sensor_write_wall_time = {}

        self.get_logger().info(
            "SEANO Logger kembali standby, menunggu armed berikutnya"
        )

    def write_mission_start_info(self, state_msg=None):
        for base_path in self.base_paths:
            mission_info_path = os.path.join(base_path, "mission_info.txt")

            with open(mission_info_path, "w") as f:
                f.write(f"Start Time: {self.start_time_obj}\n")
                f.write(f"Timezone: {self.local_timezone}\n")
                f.write("Platform: SEANO USV\n")
                f.write("Logger Mode: Armed gated\n")
                f.write("Sensor Log Rate Limit: max 1 Hz per sensor\n")
                f.write("Sensor Throttle: realtime second slot\n")
                f.write("Mission Gate: /mavros/state armed=True\n")

                if state_msg is not None:
                    f.write(f"Start MAVROS Connected: {state_msg.connected}\n")
                    f.write(f"Start MAVROS Armed: {state_msg.armed}\n")
                    f.write(f"Start MAVROS Mode: {state_msg.mode}\n")

    def write_mission_end_info(self, reason):
        end_time_obj = datetime.now()

        for base_path in self.base_paths:
            mission_info_path = os.path.join(base_path, "mission_info.txt")

            try:
                with open(mission_info_path, "a") as f:
                    f.write(f"End Time: {end_time_obj}\n")
                    f.write(f"Stop Reason: {reason}\n")
                    f.write(f"End MAVROS Connected: {self.last_connected_state}\n")
                    f.write(f"End MAVROS Armed: {self.last_armed_state}\n")
                    f.write(f"End MAVROS Mode: {self.last_flight_mode}\n")
            except Exception as e:
                self.get_logger().error(
                    f"Failed writing mission end info: {e}"
                )

    # =========================================================
    # Utility
    # =========================================================
    def get_local_timestamp(self):
        now = datetime.now()
        return now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

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

                # Umumnya millidegree Celsius
                if abs(temp_value) > 1000.0:
                    temp_c = temp_value / 1000.0
                else:
                    temp_c = temp_value

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
                        self.get_logger().info(
                            f"Jetson temperature source: {zone_type}"
                        )
                    return temp_c

        zone_type, temp_c = valid_entries[0]

        if self.jetson_temp_source != zone_type:
            self.jetson_temp_source = zone_type
            self.get_logger().info(
                f"Jetson temperature source: {zone_type}"
            )

        return temp_c

    def monitor_external_storage(self):
        if not self.logging_active:
            return

        if not self.enable_external_logging or not self.external_ready:
            return

        if not self.is_path_writable(self.external_mount_point):
            if not self.external_fail_reported:
                self.get_logger().fatal(
                    f"SSD external terputus / tidak writable lagi: "
                    f"{self.external_mount_point}"
                )
                self.get_logger().fatal(
                    "Logging ke SSD bermasalah. Local mungkin masih aktif, "
                    "tapi external gagal."
                )

                self.external_fail_reported = True
                self.external_failed_runtime = True

    # =========================================================
    # Metrics logger
    # =========================================================
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
                "WriteSpeed(Bps)\t"
                "CPU(%)\t"
                "RAM(%)\t"
                "JetsonTemp(C)\t"
                "GPS_Hz\t"
                "IMU_Hz\t"
                "CTD_Hz\t"
                "ADCP_Hz\t"
                "BATTERY_Hz\t"
                "SBES_Hz\n\n"
                "[Data]\n"
            )

            metrics_csv_file.write(
                "timestamp,write_speed_Bps,cpu_percent,ram_percent,"
                "jetson_temp_c,gps_hz,imu_hz,ctd_hz,adcp_hz,battery_hz,sbes_hz\n"
            )

            self.metrics_log_files.append(metrics_log_file)
            self.metrics_csv_files.append(metrics_csv_file)

    # =========================================================
    # Sensor detection
    # =========================================================
    def detect_and_initialize_sensors(self):
        if not self.logging_active:
            return

        topics = dict(self.get_topic_names_and_types())

        if '/mavros/global_position/raw/fix' in topics and 'gps' not in self.detected_sensors:
            self.init_sensor(
                "gps",
                "GPS",
                f"Timestamp({self.local_timezone})\tLatitude\tLongitude\tAltitude",
                "timestamp,latitude,longitude,altitude"
            )

            self.subscriptions_map['gps'] = self.create_subscription(
                NavSatFix,
                '/mavros/global_position/raw/fix',
                self.gps_callback,
                qos_profile_sensor_data
            )

            self.detected_sensors.add('gps')
            self.get_logger().info("GPS detected")

        if '/mavros/imu/data' in topics and 'imu' not in self.detected_sensors:
            self.init_sensor(
                "imu",
                "IMU",
                f"Timestamp({self.local_timezone})\tAccX\tAccY\tAccZ",
                "timestamp,acc_x,acc_y,acc_z"
            )

            self.subscriptions_map['imu'] = self.create_subscription(
                Imu,
                '/mavros/imu/data',
                self.imu_callback,
                qos_profile_sensor_data
            )

            self.detected_sensors.add('imu')
            self.get_logger().info("IMU detected")

        if '/ctd/data' in topics and 'ctd' not in self.detected_sensors:
            self.init_sensor(
                "ctd",
                "CTD",
                f"Timestamp({self.local_timezone})\tDepth\tTemp\tCond\tSalinity\tDensity\tSoundVel",
                "timestamp,depth,temp,cond,salinity,density,soundvel"
            )

            self.subscriptions_map['ctd'] = self.create_subscription(
                Float64MultiArray,
                '/ctd/data',
                self.ctd_callback,
                50
            )

            self.detected_sensors.add('ctd')
            self.get_logger().info("CTD detected")

        if '/adcp/data' in topics and 'adcp' not in self.detected_sensors:
            self.init_sensor(
                "adcp",
                "ADCP",
                (
                    f"Timestamp({self.local_timezone})\t"
                    "NumCells\t"
                    "NumBeams\t"
                    "CellSizeM\t"
                    "BlankingDistanceM\t"
                    "HeadingDeg\t"
                    "PitchDeg\t"
                    "RollDeg\t"
                    "TemperatureC\t"
                    "SalinityPSU\t"
                    "PressureDbar\t"
                    "VelocityProfile"
                ),
                (
                    "timestamp,num_cells,num_beams,cell_size_m,"
                    "blanking_distance_m,heading_deg,pitch_deg,roll_deg,"
                    "temperature_c,salinity_psu,pressure_dbar,velocity_profile"
                )
            )

            self.subscriptions_map['adcp'] = self.create_subscription(
                Float64MultiArray,
                '/adcp/data',
                self.adcp_callback,
                10
            )

            self.detected_sensors.add('adcp')
            self.get_logger().info("ADCP detected")

        if '/battery/state' in topics and 'battery' not in self.detected_sensors:
            self.init_sensor(
                "battery",
                "Battery",
                f"Timestamp({self.local_timezone})\tVoltage(V)\tCurrent(A)\tPercentage(%)",
                "timestamp,voltage_v,current_a,percentage_percent"
            )

            self.subscriptions_map['battery'] = self.create_subscription(
                BatteryState,
                '/battery/state',
                self.battery_callback,
                qos_profile_sensor_data
            )

            self.detected_sensors.add('battery')
            self.get_logger().info("Battery detected from /battery/state")

        if '/sbes/data' in topics and 'sbes' not in self.detected_sensors:
            self.init_sensor(
                "sbes",
                "SBES",
                f"Timestamp({self.local_timezone})\tDepth\tWaterTemp\tQualityFlag",
                "timestamp,depth,water_temp,quality_flag"
            )

            self.subscriptions_map['sbes'] = self.create_subscription(
                Float64MultiArray,
                '/sbes/data',
                self.sbes_callback,
                10
            )

            self.detected_sensors.add('sbes')
            self.get_logger().info("SBES detected")

    def init_sensor(self, key, name, log_columns, csv_columns):
        if not self.logging_active or not self.base_paths:
            return

        self.files[key] = []
        self.csv_files[key] = []
        self.sample_count[key] = 0
        self.last_sample_count[key] = 0
        self.sensor_log_status[key] = False

        # Supaya baris pertama setiap sensor langsung ditulis saat data pertama masuk
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

    # =========================================================
    # Safe write + realtime slot throttle
    # =========================================================
    def write_sensor_data(self, key, log_line, csv_line):
        if not self.logging_active:
            return

        if key not in self.files:
            return

        current_slot = int(time.time())
        last_slot = self.last_sensor_write_slot.get(key, -1)

        # Maksimal 1 data per sensor per detik realtime
        if last_slot == current_slot:
            return

        self.last_sensor_write_slot[key] = current_slot
        self.last_sensor_write_wall_time[key] = time.time()

        success_count = 0
        total_targets = len(self.files[key])

        for idx, log_file in enumerate(self.files[key]):
            try:
                log_file.write(log_line)
                log_file.flush()

                self.csv_files[key][idx].write(csv_line)
                self.csv_files[key][idx].flush()

                success_count += 1

            except Exception as e:
                self.get_logger().error(
                    f"{key.upper()} logging failed on target {idx}: {e}"
                )

        if success_count > 0:
            self.bytes_written_since_last_metrics += len(log_line.encode('utf-8'))
            self.bytes_written_since_last_metrics += len(csv_line.encode('utf-8'))
            self.sample_count[key] += 1

            if not self.sensor_log_status.get(key, False):
                self.get_logger().info(f"{key.upper()} logging active")
                self.sensor_log_status[key] = True

        else:
            if self.sensor_log_status.get(key, False):
                self.get_logger().error(
                    f"{key.upper()} logging failed on all targets"
                )
                self.sensor_log_status[key] = False

        if self.enable_external_logging and self.external_ready and total_targets > 0:
            if not self.is_path_writable(self.external_mount_point):
                if not self.external_fail_reported:
                    self.get_logger().fatal(
                        f"SSD external terputus / tidak writable lagi: "
                        f"{self.external_mount_point}"
                    )
                    self.external_fail_reported = True
                    self.external_failed_runtime = True

    # =========================================================
    # Sensor callbacks
    # =========================================================
    def gps_callback(self, msg):
        t = self.get_local_timestamp()

        log_line = (
            f"{t}\t"
            f"{msg.latitude}\t"
            f"{msg.longitude}\t"
            f"{msg.altitude}\n"
        )

        csv_line = (
            f"{t},"
            f"{msg.latitude},"
            f"{msg.longitude},"
            f"{msg.altitude}\n"
        )

        self.write_sensor_data('gps', log_line, csv_line)

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

        self.write_sensor_data('imu', log_line, csv_line)

    def ctd_callback(self, msg):
        t = self.get_local_timestamp()

        if len(msg.data) >= 6:
            depth, temp, cond, salinity, density, soundvel = msg.data[:6]

            log_line = (
                f"{t}\t"
                f"{depth}\t"
                f"{temp}\t"
                f"{cond}\t"
                f"{salinity}\t"
                f"{density}\t"
                f"{soundvel}\n"
            )

            csv_line = (
                f"{t},"
                f"{depth},"
                f"{temp},"
                f"{cond},"
                f"{salinity},"
                f"{density},"
                f"{soundvel}\n"
            )

        else:
            data_str = "\t".join(map(str, msg.data))
            csv_str = ",".join(map(str, msg.data))

            log_line = f"{t}\t{data_str}\n"
            csv_line = f"{t},{csv_str}\n"

        self.write_sensor_data('ctd', log_line, csv_line)

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
            velocity_profile = data[10:]

            velocity_profile_str = ";".join(map(str, velocity_profile))

            log_line = (
                f"{t}\t"
                f"{num_cells}\t"
                f"{num_beams}\t"
                f"{cell_size_m}\t"
                f"{blanking_distance_m}\t"
                f"{heading_deg}\t"
                f"{pitch_deg}\t"
                f"{roll_deg}\t"
                f"{temperature_c}\t"
                f"{salinity_psu}\t"
                f"{pressure_dbar}\t"
                f"{velocity_profile_str}\n"
            )

            csv_line = (
                f"{t},"
                f"{num_cells},"
                f"{num_beams},"
                f"{cell_size_m},"
                f"{blanking_distance_m},"
                f"{heading_deg},"
                f"{pitch_deg},"
                f"{roll_deg},"
                f"{temperature_c},"
                f"{salinity_psu},"
                f"{pressure_dbar},"
                f"\"{velocity_profile_str}\"\n"
            )

        else:
            data_str = "\t".join(map(str, data))
            csv_str = ",".join(map(str, data))

            log_line = f"{t}\t{data_str}\n"
            csv_line = f"{t},{csv_str}\n"

        self.write_sensor_data('adcp', log_line, csv_line)

    def battery_callback(self, msg):
        t = self.get_local_timestamp()

        voltage = msg.voltage
        current = msg.current
        percentage = float(msg.percentage)

        # BatteryState ROS standar memakai 0.0 sampai 1.0.
        # File logger dibuat dalam format persen 0 sampai 100.
        if math.isnan(percentage):
            percentage_percent = math.nan
        elif percentage <= 1.0:
            percentage_percent = percentage * 100.0
        else:
            percentage_percent = percentage

        log_line = (
            f"{t}\t"
            f"{voltage:.3f}\t"
            f"{current:.3f}\t"
            f"{percentage_percent:.2f}\n"
        )

        csv_line = (
            f"{t},"
            f"{voltage:.3f},"
            f"{current:.3f},"
            f"{percentage_percent:.2f}\n"
        )

        self.write_sensor_data('battery', log_line, csv_line)

    def sbes_callback(self, msg):
        t = self.get_local_timestamp()

        if len(msg.data) >= 3:
            depth = msg.data[0]
            water_temp = msg.data[1]
            quality_flag = msg.data[2]

            log_line = (
                f"{t}\t"
                f"{depth}\t"
                f"{water_temp}\t"
                f"{quality_flag}\n"
            )

            csv_line = (
                f"{t},"
                f"{depth},"
                f"{water_temp},"
                f"{quality_flag}\n"
            )

        else:
            data_str = "\t".join(map(str, msg.data))
            csv_str = ",".join(map(str, msg.data))

            log_line = f"{t}\t{data_str}\n"
            csv_line = f"{t},{csv_str}\n"

        self.write_sensor_data('sbes', log_line, csv_line)

    # =========================================================
    # Metrics
    # =========================================================
    def get_sensor_rate(self, key, elapsed):
        last_write_time = self.last_sensor_write_wall_time.get(key, 0.0)

        if last_write_time <= 0.0:
            return 0.0

        age = time.time() - last_write_time

        # Jika sensor berhasil nulis dalam 1.5 detik terakhir,
        # anggap sensor aktif 1 Hz.
        # Ini menghindari 0.00 random akibat beda fase callback dan timer metrics.
        if age <= 1.5:
            return 1.0

        return 0.0

    def log_periodic_metrics(self):
        if not self.logging_active or not self.metrics_log_files:
            self.bytes_written_since_last_metrics = 0
            self.last_metrics_time = time.time()
            return

        now = time.time()
        elapsed = (
            now - self.last_metrics_time
            if self.last_metrics_time
            else self.metrics_interval
        )

        t = self.get_local_timestamp()

        write_speed = (
            self.bytes_written_since_last_metrics / elapsed
            if elapsed > 0
            else 0.0
        )

        cpu_usage = psutil.cpu_percent(interval=None)
        ram_usage = psutil.virtual_memory().percent

        jetson_temp_c = self.read_jetson_temperature()
        jetson_temp_str = (
            f"{jetson_temp_c:.2f}"
            if jetson_temp_c is not None
            else "nan"
        )

        gps_hz = self.get_sensor_rate('gps', elapsed)
        imu_hz = self.get_sensor_rate('imu', elapsed)
        ctd_hz = self.get_sensor_rate('ctd', elapsed)
        adcp_hz = self.get_sensor_rate('adcp', elapsed)
        battery_hz = self.get_sensor_rate('battery', elapsed)
        sbes_hz = self.get_sensor_rate('sbes', elapsed)

        log_line = (
            f"{t}\t"
            f"{write_speed:.2f}\t"
            f"{cpu_usage:.2f}\t"
            f"{ram_usage:.2f}\t"
            f"{jetson_temp_str}\t"
            f"{gps_hz:.2f}\t"
            f"{imu_hz:.2f}\t"
            f"{ctd_hz:.2f}\t"
            f"{adcp_hz:.2f}\t"
            f"{battery_hz:.2f}\t"
            f"{sbes_hz:.2f}\n"
        )

        csv_line = (
            f"{t},"
            f"{write_speed:.2f},"
            f"{cpu_usage:.2f},"
            f"{ram_usage:.2f},"
            f"{jetson_temp_str},"
            f"{gps_hz:.2f},"
            f"{imu_hz:.2f},"
            f"{ctd_hz:.2f},"
            f"{adcp_hz:.2f},"
            f"{battery_hz:.2f},"
            f"{sbes_hz:.2f}\n"
        )

        for i, metrics_log_file in enumerate(self.metrics_log_files):
            try:
                metrics_log_file.write(log_line)
                metrics_log_file.flush()

                self.metrics_csv_files[i].write(csv_line)
                self.metrics_csv_files[i].flush()

            except Exception as e:
                self.get_logger().error(
                    f"Metrics logging failed on target {i}: {e}"
                )

        self.bytes_written_since_last_metrics = 0
        self.last_metrics_time = now

    # =========================================================
    # Shutdown helpers
    # =========================================================
    def destroy_sensor_subscriptions(self):
        for key, subscription in list(self.subscriptions_map.items()):
            try:
                self.destroy_subscription(subscription)
            except Exception as e:
                self.get_logger().warning(
                    f"Failed to destroy {key} subscription: {e}"
                )

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


if __name__ == '__main__':
    main()