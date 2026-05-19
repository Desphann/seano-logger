import rclpy
from rclpy.node import Node
from std_msgs.msg import Float64MultiArray
import math
import time

class CTDSim(Node):

    def __init__(self):
        super().__init__('ctd_reader')
        self.publisher_ = self.create_publisher(Float64MultiArray, '/ctd/data', 10)
        self.start_time = time.time()
        self.timer = self.create_timer(1.0, self.publish_ctd)

    def publish_ctd(self):
        t = time.time() - self.start_time

        msg = Float64MultiArray()
        msg.data = [
            0.02 + 0.01 * math.sin(t),
            25.4,
            0.002,
            0.012,
            996.95,
            1497.8
        ]

        self.publisher_.publish(msg)

def main():
    rclpy.init()
    node = CTDSim()
    rclpy.spin(node)