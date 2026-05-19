import rclpy
from rclpy.node import Node
from sensor_msgs.msg import NavSatFix
from datetime import datetime, timezone


class GPSReader(Node):

    def __init__(self):
        super().__init__('gps_reader')

        self.create_subscription(
            NavSatFix,
            '/mavros/global_position/raw/fix',
            self.gps_callback,
            10
        )

        self.get_logger().info("🛰 GPS Reader Started")

    def convert_time(self, stamp):
        sec = stamp.sec
        nanosec = stamp.nanosec
        dt = datetime.fromtimestamp(sec, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{int(nanosec/1e6):03d}"

    def gps_callback(self, msg):

        timestamp = self.convert_time(msg.header.stamp)

        self.get_logger().info(
            f"{timestamp} | Lat: {msg.latitude:.6f} | "
            f"Lon: {msg.longitude:.6f} | Alt: {msg.altitude:.2f}"
        )


def main():
    rclpy.init()
    node = GPSReader()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()