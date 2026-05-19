import math
import random
import time

import rclpy
from rclpy.node import Node
from std_msgs.msg import Float64MultiArray


class CTDSim(Node):
    def __init__(self):
        super().__init__('ctd_reader')

        self.declare_parameter('sample_rate', 1.0)
        self.sample_rate = float(self.get_parameter('sample_rate').value)

        self.publisher_ = self.create_publisher(Float64MultiArray, '/ctd/data', 10)
        self.start_time = time.time()
        self.timer = self.create_timer(1.0 / self.sample_rate, self.publish_ctd)

        self.publish_ok_reported = False
        self.publish_error_reported = False

        # baseline dummy
        self.base_depth = 2.0          # m
        self.base_temp = 28.5          # deg C
        self.base_salinity = 32.0      # PSU
        self.base_conductivity = 4.2   # S/m kira-kira dummy
        self.base_density = 1020.0     # kg/m3
        self.base_soundvel = 1535.0    # m/s

        self.get_logger().info(
            f"CTD Reader started | topic=/ctd/data | sample_rate={self.sample_rate} Hz"
        )

    def publish_ctd(self):
        try:
            t = time.time() - self.start_time

            # depth berubah pelan seperti kendaraan bergerak di perairan dangkal
            depth = self.base_depth + 1.5 * math.sin(t * 0.08) + 0.3 * math.sin(t * 0.21)
            depth += random.uniform(-0.03, 0.03)
            depth = max(0.1, depth)

            # suhu sedikit turun saat depth bertambah + gelombang lambat
            temperature = self.base_temp - 0.15 * depth + 0.25 * math.sin(t * 0.03)
            temperature += random.uniform(-0.03, 0.03)

            # salinity berubah halus
            salinity = self.base_salinity + 0.4 * math.sin(t * 0.025 + 1.2)
            salinity += 0.1 * math.cos(t * 0.06)
            salinity += random.uniform(-0.02, 0.02)

            # conductivity mengikuti salinity dan temperature secara sederhana
            conductivity = self.base_conductivity
            conductivity += 0.03 * (temperature - self.base_temp)
            conductivity += 0.02 * (salinity - self.base_salinity)
            conductivity += 0.05 * math.sin(t * 0.05)
            conductivity += random.uniform(-0.01, 0.01)

            # density dummy yang lebih logis:
            # naik saat salinity naik, turun saat temperature naik
            density = self.base_density
            density += 0.78 * (salinity - self.base_salinity)
            density -= 0.22 * (temperature - self.base_temp)
            density += 0.05 * depth
            density += random.uniform(-0.03, 0.03)

            # sound velocity dummy:
            # umumnya naik dengan temperature, salinity, dan sedikit dengan pressure/depth
            soundvel = self.base_soundvel
            soundvel += 2.8 * (temperature - self.base_temp)
            soundvel += 1.2 * (salinity - self.base_salinity)
            soundvel += 0.18 * depth
            soundvel += random.uniform(-0.1, 0.1)

            msg = Float64MultiArray()
            msg.data = [
                round(depth, 3),
                round(temperature, 3),
                round(conductivity, 3),
                round(salinity, 3),
                round(density, 3),
                round(soundvel, 3)
            ]

            self.publisher_.publish(msg)

            if not self.publish_ok_reported:
                self.get_logger().info("CTD dummy publish active")
                self.publish_ok_reported = True
                self.publish_error_reported = False

        except Exception as e:
            if not self.publish_error_reported:
                self.get_logger().error(f"CTD publish failed: {e}")
                self.publish_error_reported = True
                self.publish_ok_reported = False


def main():
    rclpy.init()
    node = CTDSim()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()