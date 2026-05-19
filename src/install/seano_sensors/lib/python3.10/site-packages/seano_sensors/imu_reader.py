import rclpy
from rclpy.node import Node
from sensor_msgs.msg import Imu
from datetime import datetime, timezone


class IMUReader(Node):

    def __init__(self):
        super().__init__('imu_reader')

        self.create_subscription(
            Imu,
            '/mavros/imu/data',
            self.imu_callback,
            50
        )

        self.get_logger().info("🧭 IMU Reader Started")

    def convert_time(self, stamp):
        sec = stamp.sec
        nanosec = stamp.nanosec
        dt = datetime.fromtimestamp(sec, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{int(nanosec/1e6):03d}"

    def imu_callback(self, msg):

        timestamp = self.convert_time(msg.header.stamp)

        self.get_logger().info(
            f"{timestamp} | "
            f"Ax: {msg.linear_acceleration.x:.3f} | "
            f"Ay: {msg.linear_acceleration.y:.3f} | "
            f"Az: {msg.linear_acceleration.z:.3f}"
        )


def main():
    rclpy.init()
    node = IMUReader()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()