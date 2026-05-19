import rclpy
from rclpy.node import Node
from sensor_msgs.msg import BatteryState
from datetime import datetime, timezone


class BatteryReader(Node):
    def __init__(self):
        super().__init__('battery_reader')

        self.create_subscription(
            BatteryState,
            '/battery/state',
            self.battery_callback,
            10
        )

        self.get_logger().info("Battery Reader Started")

    def convert_time(self, stamp):
        sec = stamp.sec
        nanosec = stamp.nanosec
        dt = datetime.fromtimestamp(sec, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{int(nanosec/1e6):03d}"

    def battery_callback(self, msg):
        timestamp = self.convert_time(msg.header.stamp)
        self.get_logger().info(
            f"{timestamp} | "
            f"Voltage: {msg.voltage:.3f} | "
            f"Current: {msg.current:.3f} | "
            f"Percentage: {msg.percentage:.3f}"
        )


def main():
    rclpy.init()
    node = BatteryReader()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()