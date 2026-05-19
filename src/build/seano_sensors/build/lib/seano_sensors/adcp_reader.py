import rclpy
from rclpy.node import Node
from std_msgs.msg import Float64MultiArray
import math
import time

class ADCPSim(Node):

    def __init__(self):
        super().__init__('adcp_reader')
        self.publisher_ = self.create_publisher(Float64MultiArray, '/adcp/data', 10)
        self.start_time = time.time()
        self.timer = self.create_timer(1.0, self.publish_adcp)

    def publish_adcp(self):
        t = time.time() - self.start_time

        data = [5.0, 4.0]

        for cell in range(5):
            for beam in range(4):
                velocity = 0.5 * math.sin(t + cell)
                data.append(velocity)

        msg = Float64MultiArray()
        msg.data = data

        self.publisher_.publish(msg)

def main():
    rclpy.init()
    node = ADCPSim()
    rclpy.spin(node)