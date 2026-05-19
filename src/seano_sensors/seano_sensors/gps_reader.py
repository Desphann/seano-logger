import time
from datetime import datetime, timezone

import rclpy
from rclpy.node import Node
from sensor_msgs.msg import NavSatFix


class GPSReader(Node):
    def __init__(self):
        super().__init__('gps_reader')

        self.declare_parameter('sample_rate', 1.0)
        self.sample_rate = float(self.get_parameter('sample_rate').value)
        self.min_period = 1.0 / self.sample_rate

        self.timeout_sec = 5.0

        self.last_process_time = None
        self.last_msg_time = None

        # Flag supaya terminal tidak spam
        self.waiting_logged = False
        self.first_data_logged = False
        self.timeout_logged = False

        self.is_connected = False

        self.create_subscription(
            NavSatFix,
            '/mavros/global_position/raw/fix',
            self.gps_callback,
            10
        )

        self.create_timer(1.0, self.check_status)

        self.get_logger().info(
            f"GPS Reader started | topic=/mavros/global_position/raw/fix | sample_rate={self.sample_rate} Hz"
        )

        # Waiting info hanya sekali
        self.get_logger().info("GPS status: waiting for first data...")
        self.waiting_logged = True

    def convert_time(self, stamp):
        sec = stamp.sec
        nanosec = stamp.nanosec

        if sec == 0 and nanosec == 0:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        dt = datetime.fromtimestamp(sec, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{int(nanosec / 1e6):03d}"

    def gps_callback(self, msg):
        now = time.time()
        self.last_msg_time = now

        # Batasi proses internal sesuai sample_rate
        if self.last_process_time is not None:
            if (now - self.last_process_time) < self.min_period:
                return

        self.last_process_time = now
        self.is_connected = True

        # Print hanya sekali saat data pertama berhasil diterima
        if not self.first_data_logged:
            timestamp = self.convert_time(msg.header.stamp)
            self.get_logger().info(
                f"GPS data received from /mavros/global_position/raw/fix at {timestamp}"
            )
            self.first_data_logged = True
            self.timeout_logged = False

        # Setelah ini tidak ada log lagi untuk setiap data GPS.
        # Data tetap diterima oleh node, hanya terminal tidak spam.

    def check_status(self):
        now = time.time()

        # Kalau belum pernah ada data, jangan print berulang-ulang
        if self.last_msg_time is None:
            return

        # Timeout hanya dilaporkan sekali
        if (now - self.last_msg_time) > self.timeout_sec:
            if not self.timeout_logged:
                self.get_logger().error(
                    "GPS timeout | no data received in the last 5 seconds"
                )
                self.timeout_logged = True
                self.is_connected = False


def main(args=None):
    rclpy.init(args=args)

    node = GPSReader()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass

    node.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()