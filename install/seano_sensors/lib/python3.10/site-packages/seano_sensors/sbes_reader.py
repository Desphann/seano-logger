import math
import random
import time

import rclpy
from rclpy.node import Node
from std_msgs.msg import Float64MultiArray


class SBESSim(Node):
    def __init__(self):
        super().__init__('sbes_reader')

        self.declare_parameter('sample_rate', 1.0)
        self.sample_rate = float(self.get_parameter('sample_rate').value)

        self.publisher_ = self.create_publisher(Float64MultiArray, '/sbes/data', 10)
        self.timer = self.create_timer(1.0 / self.sample_rate, self.publish_sbes)

        self.start_time = time.time()
        self.base_depth = 12.0
        self.depth_amp = 1.5
        self.temp_base = 28.0

        self.publish_ok_reported = False
        self.publish_error_reported = False

        self.get_logger().info(
            f"SBES Reader started | topic=/sbes/data | sample_rate={self.sample_rate} Hz"
        )

    def publish_sbes(self):
        try:
            t = time.time() - self.start_time

            depth = self.base_depth + self.depth_amp * math.sin(t * 0.1)
            depth += random.uniform(-0.05, 0.05)

            water_temp = self.temp_base + 0.3 * math.sin(t * 0.03)
            water_temp += random.uniform(-0.02, 0.02)

            quality_flag = 1.0

            msg = Float64MultiArray()
            msg.data = [
                round(depth, 3),
                round(water_temp, 3),
                quality_flag
            ]

            self.publisher_.publish(msg)

            if not self.publish_ok_reported:
                self.get_logger().info("SBES dummy publish active")
                self.publish_ok_reported = True
                self.publish_error_reported = False

        except Exception as e:
            if not self.publish_error_reported:
                self.get_logger().error(f"SBES publish failed: {e}")
                self.publish_error_reported = True
                self.publish_ok_reported = False


def main():
    rclpy.init()
    node = SBESSim()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()