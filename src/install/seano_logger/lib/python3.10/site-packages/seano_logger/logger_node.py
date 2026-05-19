import os
import time
from datetime import datetime

import psutil
import rclpy
from rclpy.node import Node
from rclpy.qos import qos_profile_sensor_data
from sensor_msgs.msg import NavSatFix, Imu, BatteryState
from std_msgs.msg import Float64MultiArray


class SeanoLogger(Node):
    def __init__(self):
        super().__init__('logger_node')

        # Konfigurasi lokasi penyimpanan dan interval flush buffer ke file
        self.mount_point = "/media/raihan/SEANO"
        self.flush_interval = 3.0

        # Ambil waktu mulai misi dan timezone lokal
        self.start_time_obj = datetime.now()
        self.local_timezone = time.tzname[0]

        # Ambil tanggal untuk struktur folder log
        year = self.start_time_obj.strftime("%Y")
        month = self.start_time_obj.strftime("%m")
        day = self.start_time_obj.strftime("%d")

        # Nama folder misi dibuat dari jam mulai misi
        self.mission_id = self.start_time_obj.strftime(
            f"MISSION_START_%H-%M-%S_{self.local_timezone}"
        )

        # Path lengkap folder mission
        self.base_path = os.path.join(
            self.mount_point,
            "SEANO_MISSIONS",
            year,
            month,
            day,
            self.mission_id
        )
        os.makedirs(self.base_path, exist_ok=True)

        # Simpan informasi dasar misi
        with open(os.path.join(self.base_path, "mission_info.txt"), "w") as f:
            f.write(f"Start Time: {self.start_time_obj}\n")
            f.write(f"Timezone: {self.local_timezone}\n")
            f.write("Platform: SEANO USV\n")

        # Struktur utama logger
        self.files = {}
        self.csv_files = {}
        self.buffers = {}
        self.sample_count = {}
        self.detected_sensors = set()
        self.subscriptions_map = {}

        # Untuk metrics sistem
        self.metrics_log_file = None
        self.metrics_csv_file = None
        self.last_sample_count = {}
        self.last_flush_time = time.time()

        self.init_metrics_logger()

        # Prime cpu_percent supaya pembacaan berikutnya lebih stabil
        psutil.cpu_percent(interval=None)

        # Timer
        self.create_timer(2.0, self.detect_and_initialize_sensors)
        self.create_timer(self.flush_interval, self.flush_buffers)

        self.get_logger().info(f"Mission folder: {self.base_path}")
        self.get_logger().info(f"Timezone: {self.local_timezone}")
        self.get_logger().info("SEANO Logger Started")

    def get_local_timestamp(self):
        # Timestamp lokal dengan presisi milidetik
        now = datetime.now()
        return now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def init_metrics_logger(self):
        # File tambahan untuk metrics sistem
        metrics_log_path = os.path.join(self.base_path, "system_metrics.log")
        metrics_csv_path = os.path.join(self.base_path, "system_metrics.csv")

        self.metrics_log_file = open(metrics_log_path, "w")
        self.metrics_csv_file = open(metrics_csv_path, "w")

        self.metrics_log_file.write(
            "[System]\n"
            "Platform=SEANO USV\n"
            f"Start_Time={self.start_time_obj.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Timezone={self.local_timezone}\n\n"
            "[Sensor]\n"
            "Name=System Metrics\n\n"
            "[Columns]\n"
            f"Timestamp({self.local_timezone})\tWriteSpeed(Bps)\tCPU(%)\tRAM(%)\tGPS_Hz\tIMU_Hz\tCTD_Hz\tADCP_Hz\tBATTERY_Hz\n\n"
            "[Data]\n"
        )

        self.metrics_csv_file.write(
            "timestamp,write_speed_Bps,cpu_percent,ram_percent,gps_hz,imu_hz,ctd_hz,adcp_hz,battery_hz\n"
        )

    def detect_and_initialize_sensors(self):
        # Ambil semua topic aktif di ROS2
        topics = dict(self.get_topic_names_and_types())

        # GPS sensor
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

        # IMU sensor
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

        # CTD sensor
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

        # ADCP sensor
        if '/adcp/data' in topics and 'adcp' not in self.detected_sensors:
            self.init_sensor(
                "adcp",
                "ADCP",
                f"Timestamp({self.local_timezone})\tCellCount\tBeamCount\tVelocityData",
                "timestamp,data"
            )
            self.subscriptions_map['adcp'] = self.create_subscription(
                Float64MultiArray,
                '/adcp/data',
                self.adcp_callback,
                10
            )
            self.detected_sensors.add('adcp')

        # Battery sensor
        if '/battery/state' in topics and 'battery' not in self.detected_sensors:
            self.init_sensor(
                "battery",
                "Battery",
                f"Timestamp({self.local_timezone})\tVoltage\tCurrent\tPercentage",
                "timestamp,voltage,current,percentage"
            )
            self.subscriptions_map['battery'] = self.create_subscription(
                BatteryState,
                '/battery/state',
                self.battery_callback,
                10
            )
            self.detected_sensors.add('battery')

    def init_sensor(self, key, name, log_columns, csv_columns):
        # Buat file .log dan .csv untuk sensor yang baru terdeteksi
        log_path = os.path.join(self.base_path, f"{key}.log")
        csv_path = os.path.join(self.base_path, f"{key}.csv")

        log_file = open(log_path, "w")
        csv_file = open(csv_path, "w")

        # Header file .log
        log_file.write(
            "[System]\n"
            "Platform=SEANO USV\n"
            f"Start_Time={self.start_time_obj.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Timezone={self.local_timezone}\n\n"
            "[Sensor]\n"
            f"Name={name}\n\n"
            "[Columns]\n"
            f"{log_columns}\n\n"
            "[Data]\n"
        )

        # Header file .csv
        csv_file.write(csv_columns + "\n")

        # Simpan reference file dan buffer
        self.files[key] = log_file
        self.csv_files[key] = csv_file
        self.buffers[key] = []
        self.sample_count[key] = 0
        self.last_sample_count[key] = 0

        self.get_logger().info(f"{name} detected")

    def gps_callback(self, msg):
        t = self.get_local_timestamp()
        log_line = f"{t}\t{msg.latitude}\t{msg.longitude}\t{msg.altitude}\n"
        csv_line = f"{t},{msg.latitude},{msg.longitude},{msg.altitude}\n"
        self.buffers['gps'].append((log_line, csv_line))
        self.sample_count['gps'] += 1

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
        self.buffers['imu'].append((log_line, csv_line))
        self.sample_count['imu'] += 1

    def ctd_callback(self, msg):
        t = self.get_local_timestamp()
        data_str = "\t".join(map(str, msg.data))
        csv_str = ",".join(map(str, msg.data))
        log_line = f"{t}\t{data_str}\n"
        csv_line = f"{t},{csv_str}\n"
        self.buffers['ctd'].append((log_line, csv_line))
        self.sample_count['ctd'] += 1

    def adcp_callback(self, msg):
        t = self.get_local_timestamp()
        data_str = "\t".join(map(str, msg.data))
        csv_str = ",".join(map(str, msg.data))
        log_line = f"{t}\t{data_str}\n"
        csv_line = f"{t},{csv_str}\n"
        self.buffers['adcp'].append((log_line, csv_line))
        self.sample_count['adcp'] += 1

    def battery_callback(self, msg):
        t = self.get_local_timestamp()
        log_line = f"{t}\t{msg.voltage}\t{msg.current}\t{msg.percentage}\n"
        csv_line = f"{t},{msg.voltage},{msg.current},{msg.percentage}\n"
        self.buffers['battery'].append((log_line, csv_line))
        self.sample_count['battery'] += 1

    def get_sensor_rate(self, key, elapsed):
        current = self.sample_count.get(key, 0)
        previous = self.last_sample_count.get(key, 0)
        rate = (current - previous) / elapsed if elapsed > 0 else 0.0
        self.last_sample_count[key] = current
        return rate

    def log_system_metrics(self, bytes_written, elapsed):
        t = self.get_local_timestamp()

        write_speed = bytes_written / elapsed if elapsed > 0 else 0.0  # Bps
        cpu_usage = psutil.cpu_percent(interval=None)
        ram_usage = psutil.virtual_memory().percent

        gps_hz = self.get_sensor_rate('gps', elapsed)
        imu_hz = self.get_sensor_rate('imu', elapsed)
        ctd_hz = self.get_sensor_rate('ctd', elapsed)
        adcp_hz = self.get_sensor_rate('adcp', elapsed)
        battery_hz = self.get_sensor_rate('battery', elapsed)

        log_line = (
            f"{t}\t"
            f"{write_speed:.2f}\t"
            f"{cpu_usage:.2f}\t"
            f"{ram_usage:.2f}\t"
            f"{gps_hz:.2f}\t"
            f"{imu_hz:.2f}\t"
            f"{ctd_hz:.2f}\t"
            f"{adcp_hz:.2f}\t"
            f"{battery_hz:.2f}\n"
        )

        csv_line = (
            f"{t},"
            f"{write_speed:.2f},"
            f"{cpu_usage:.2f},"
            f"{ram_usage:.2f},"
            f"{gps_hz:.2f},"
            f"{imu_hz:.2f},"
            f"{ctd_hz:.2f},"
            f"{adcp_hz:.2f},"
            f"{battery_hz:.2f}\n"
        )

        self.metrics_log_file.write(log_line)
        self.metrics_csv_file.write(csv_line)
        self.metrics_log_file.flush()
        self.metrics_csv_file.flush()

    def flush_buffers(self):
        # Tulis seluruh isi buffer ke file
        bytes_written = 0
        now = time.time()
        elapsed = now - self.last_flush_time if self.last_flush_time else self.flush_interval

        for key in self.buffers:
            if self.buffers[key]:
                for log_line, csv_line in self.buffers[key]:
                    self.files[key].write(log_line)
                    self.csv_files[key].write(csv_line)

                    bytes_written += len(log_line.encode('utf-8'))
                    bytes_written += len(csv_line.encode('utf-8'))

                self.files[key].flush()
                self.csv_files[key].flush()
                self.buffers[key].clear()

        # Log metrics sistem
        self.log_system_metrics(bytes_written, elapsed)
        self.last_flush_time = now

        self.get_logger().info("Buffers flushed")

    def destroy_node(self):
        # Pastikan semua data terakhir ditulis sebelum node ditutup
        self.flush_buffers()

        for key in self.files:
            self.files[key].close()
            self.csv_files[key].close()

        if self.metrics_log_file:
            self.metrics_log_file.close()
        if self.metrics_csv_file:
            self.metrics_csv_file.close()

        os.sync()
        super().destroy_node()


def main():
    rclpy.init()
    node = SeanoLogger()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass

    node.destroy_node()
    rclpy.shutdown()