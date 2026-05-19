import rclpy
from rclpy.node import Node
from sensor_msgs.msg import NavSatFix
import math
import time

class GPSSim(Node):

    def __init__(self):
        super().__init__('gps_reader')

        self.publisher_ = self.create_publisher(
            NavSatFix,
            '/gps/fix',
            10
        )

        self.start_time = time.time()
        self.timer = self.create_timer(0.2, self.publish_gps)  # 5 Hz

        self.base_lat = -6.890000
        self.base_lon = 107.610000

    def publish_gps(self):
        msg = NavSatFix()

        t = time.time() - self.start_time

        msg.latitude = self.base_lat + 0.00001 * math.sin(0.01 * t)
        msg.longitude = self.base_lon + 0.00001 * math.cos(0.01 * t)
        msg.altitude = 0.5

        msg.header.stamp = self.get_clock().now().to_msg()
        msg.header.frame_id = "gps_link"

        self.publisher_.publish(msg)

def main():
    rclpy.init()
    node = GPSSim()
    rclpy.spin(node)