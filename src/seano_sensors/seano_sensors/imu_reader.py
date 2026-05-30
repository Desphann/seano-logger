#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IMU Reader - Quiet Terminal - Fixed QoS
=======================================

Subscribe:
    /mavros/imu/data  (sensor_msgs/Imu)

Terminal output:
    IMU_READER: NOT READY
    IMU_READER: READY

READY berarti:
    Pesan IMU dari MAVROS sudah diterima dan stream belum timeout.

Perbaikan penting:
    Subscriber memakai qos_profile_sensor_data supaya cocok dengan MAVROS
    sensor topic yang umumnya BEST_EFFORT. Kalau memakai angka biasa 50,
    ROS2 default-nya RELIABLE dan bisa QoS mismatch.
"""

import time

import rclpy
from rclpy.node import Node
from rclpy.qos import qos_profile_sensor_data
from sensor_msgs.msg import Imu


class IMUReader(Node):
    def __init__(self):
        super().__init__("imu_reader")

        self.declare_parameter("sample_rate", 1.0)
        self.declare_parameter("topic", "/mavros/imu/data")
        self.declare_parameter("timeout_sec", 5.0)

        self.sample_rate = float(self.get_parameter("sample_rate").value)
        self.topic = str(self.get_parameter("topic").value)
        self.timeout_sec = float(self.get_parameter("timeout_sec").value)

        if self.sample_rate <= 0.0:
            self.sample_rate = 1.0

        self.min_period = 1.0 / self.sample_rate

        self.last_process_time = None
        self.last_msg_time = None

        self.ready = False
        self.last_status = None

        self.create_subscription(
            Imu,
            self.topic,
            self.imu_callback,
            qos_profile_sensor_data,
        )

        self.create_timer(1.0, self.check_status)

        self.set_ready(False)

    def set_ready(self, ready: bool):
        status = "READY" if ready else "NOT READY"

        if status == self.last_status:
            return

        self.ready = ready
        self.last_status = status
        self.get_logger().info(f"IMU_READER: {status}")

    def imu_callback(self, msg):
        now = time.time()
        self.last_msg_time = now

        # READY harus berdasarkan stream masuk, bukan sample-rate processing.
        self.set_ready(True)

        # Sample-rate gate tetap dipertahankan jika nanti ada proses internal.
        if self.last_process_time is not None:
            if (now - self.last_process_time) < self.min_period:
                return

        self.last_process_time = now

    def check_status(self):
        if self.last_msg_time is None:
            self.set_ready(False)
            return

        age = time.time() - self.last_msg_time

        if age > self.timeout_sec:
            self.set_ready(False)


def main(args=None):
    rclpy.init(args=args)

    node = IMUReader()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()