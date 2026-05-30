#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Battery Reader - Quiet Terminal
===============================

Input:
    MQTT topic berisi data battery JSON.

Publish:
    /battery/state  (sensor_msgs/BatteryState)

Terminal output:
    BATTERY_READER: NOT READY
    BATTERY_READER: READY

READY berarti:
    MQTT connected dan minimal satu data battery valid sudah diterima,
    serta data belum stale.

NOT READY berarti:
    MQTT belum connected, belum ada data battery, disconnected,
    atau data MQTT sudah stale.
"""

import json
import math
import time

import paho.mqtt.client as mqtt

import rclpy
from rclpy.node import Node
from sensor_msgs.msg import BatteryState


class BatteryMonitor(Node):
    def __init__(self):
        super().__init__("battery_reader")

        # ============================================================
        # PARAMETERS
        # ============================================================
        self.declare_parameter("mqtt_broker", "mqtt.seano.cloud")
        self.declare_parameter("mqtt_port", 8883)
        self.declare_parameter("mqtt_username", "seanomqtt")
        self.declare_parameter("mqtt_password", "Seano2025*")
        self.declare_parameter("mqtt_battery_topic", "seano/USV-001/battery")

        self.declare_parameter("ros_battery_topic", "/battery/state")
        self.declare_parameter("publish_rate_hz", 1.0)
        self.declare_parameter("stale_timeout_sec", 10.0)

        self.mqtt_broker = str(self.get_parameter("mqtt_broker").value)
        self.mqtt_port = int(self.get_parameter("mqtt_port").value)
        self.mqtt_username = str(self.get_parameter("mqtt_username").value)
        self.mqtt_password = str(self.get_parameter("mqtt_password").value)
        self.mqtt_battery_topic = str(self.get_parameter("mqtt_battery_topic").value)

        self.ros_battery_topic = str(self.get_parameter("ros_battery_topic").value)
        self.publish_rate_hz = float(self.get_parameter("publish_rate_hz").value)
        self.stale_timeout_sec = float(self.get_parameter("stale_timeout_sec").value)

        if self.publish_rate_hz <= 0.0:
            self.publish_rate_hz = 1.0

        self.publish_period = 1.0 / self.publish_rate_hz

        # ============================================================
        # ROS PUBLISHER
        # ============================================================
        self.battery_pub = self.create_publisher(
            BatteryState,
            self.ros_battery_topic,
            10,
        )

        # ============================================================
        # STATE
        # ============================================================
        self.latest_percentage = math.nan
        self.latest_voltage = math.nan
        self.latest_current = math.nan
        self.latest_mqtt_time = None

        self.mqtt_connected = False
        self.ready = False
        self.last_status = None

        self.create_timer(
            self.publish_period,
            self.publish_battery_timer,
        )

        self.create_timer(
            1.0,
            self.check_status,
        )

        # ============================================================
        # MQTT CLIENT
        # ============================================================
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)

        self.mqtt_client.username_pw_set(
            self.mqtt_username,
            self.mqtt_password,
        )

        self.mqtt_client.tls_set()
        self.mqtt_client.tls_insecure_set(True)

        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_disconnect = self.on_mqtt_disconnect
        self.mqtt_client.on_message = self.on_mqtt_message

        self.set_ready(False)

        self.mqtt_client.connect_async(
            self.mqtt_broker,
            self.mqtt_port,
            60,
        )

        self.mqtt_client.loop_start()

    # ============================================================
    # STATUS
    # ============================================================
    def set_ready(self, ready: bool):
        if ready == self.ready and self.last_status is not None:
            return

        self.ready = ready
        status = "READY" if ready else "NOT READY"

        if status == self.last_status:
            return

        self.last_status = status
        self.get_logger().info(f"BATTERY_READER: {status}")

    def update_ready_state(self):
        if not self.mqtt_connected:
            self.set_ready(False)
            return

        if self.latest_mqtt_time is None:
            self.set_ready(False)
            return

        data_age = time.time() - self.latest_mqtt_time

        if data_age > self.stale_timeout_sec:
            self.set_ready(False)
            return

        self.set_ready(True)

    # ============================================================
    # MQTT CALLBACKS
    # ============================================================
    def on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.mqtt_connected = True
            client.subscribe(self.mqtt_battery_topic)
        else:
            self.mqtt_connected = False

        self.update_ready_state()

    def on_mqtt_disconnect(self, client, userdata, rc):
        self.mqtt_connected = False
        self.update_ready_state()

    def on_mqtt_message(self, client, userdata, msg):
        try:
            payload = msg.payload.decode()
            data = json.loads(payload)

            percentage = data.get("percentage", math.nan)
            voltage = data.get("voltage", math.nan)
            current = data.get("current", math.nan)

            self.latest_percentage = self.normalize_percentage(percentage)
            self.latest_voltage = self.to_float_or_nan(voltage)
            self.latest_current = self.to_float_or_nan(current)
            self.latest_mqtt_time = time.time()

            self.update_ready_state()

        except Exception:
            self.set_ready(False)

    # ============================================================
    # ROS PUBLISH
    # ============================================================
    def publish_battery_timer(self):
        if self.latest_mqtt_time is None:
            return

        battery_msg = BatteryState()
        battery_msg.header.stamp = self.get_clock().now().to_msg()

        battery_msg.percentage = self.latest_percentage
        battery_msg.voltage = self.latest_voltage
        battery_msg.current = self.latest_current

        self.battery_pub.publish(battery_msg)

    def check_status(self):
        self.update_ready_state()

    # ============================================================
    # CONVERSION
    # ============================================================
    @staticmethod
    def normalize_percentage(value):
        if value is None:
            return math.nan

        try:
            percentage = float(value)

            if percentage > 1.0:
                return percentage / 100.0

            return percentage

        except (TypeError, ValueError):
            return math.nan

    @staticmethod
    def to_float_or_nan(value):
        if value is None:
            return math.nan

        try:
            return float(value)
        except (TypeError, ValueError):
            return math.nan

    # ============================================================
    # SHUTDOWN
    # ============================================================
    def destroy_node(self):
        try:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        except Exception:
            pass

        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)

    node = BatteryMonitor()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()