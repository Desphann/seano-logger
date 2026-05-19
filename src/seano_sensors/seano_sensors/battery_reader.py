import json
import math
import time

import paho.mqtt.client as mqtt

import rclpy
from rclpy.node import Node
from sensor_msgs.msg import BatteryState


class BatteryMonitor(Node):
    def __init__(self):
        super().__init__('battery_reader')

        # =========================
        # MQTT CONFIG
        # =========================
        self.mqtt_broker = 'mqtt.seano.cloud'
        self.mqtt_port = 8883
        self.mqtt_username = 'seanomqtt'
        self.mqtt_password = 'Seano2025*'
        self.mqtt_battery_topic = 'seano/USV-001/battery'

        # =========================
        # ROS CONFIG
        # =========================
        self.ros_battery_topic = '/battery/state'
        self.publish_rate_hz = 1.0
        self.publish_period = 1.0 / self.publish_rate_hz

        self.battery_pub = self.create_publisher(
            BatteryState,
            self.ros_battery_topic,
            10
        )

        # =========================
        # LATEST BATTERY DATA
        # =========================
        # MQTT hanya update variable ini.
        # ROS publish dilakukan oleh timer 1 Hz.
        self.latest_percentage = math.nan
        self.latest_voltage = math.nan
        self.latest_current = math.nan
        self.latest_mqtt_time = None

        # Supaya terminal tidak spam
        self.battery_data_received_logged = False
        self.stale_data_warned = False

        # Timer publish ROS battery stabil 1 Hz
        self.create_timer(
            self.publish_period,
            self.publish_battery_timer
        )

        # =========================
        # MQTT CLIENT SETUP
        # =========================
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)

        self.mqtt_client.username_pw_set(
            self.mqtt_username,
            self.mqtt_password
        )

        # Port 8883 menggunakan TLS
        self.mqtt_client.tls_set()
        self.mqtt_client.tls_insecure_set(True)

        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_disconnect = self.on_mqtt_disconnect
        self.mqtt_client.on_message = self.on_mqtt_message

        self.get_logger().info(
            f'Connecting to MQTT broker {self.mqtt_broker}:{self.mqtt_port}'
        )

        self.mqtt_client.connect_async(
            self.mqtt_broker,
            self.mqtt_port,
            60
        )

        self.mqtt_client.loop_start()

        self.get_logger().info(
            f'Battery reader started | MQTT {self.mqtt_battery_topic} '
            f'-> ROS {self.ros_battery_topic} at {self.publish_rate_hz:.1f} Hz'
        )

    def on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.get_logger().info('MQTT connected successfully')

            client.subscribe(self.mqtt_battery_topic)

            self.get_logger().info(
                f'Subscribed to MQTT topic: {self.mqtt_battery_topic}'
            )
        else:
            self.get_logger().error(
                f'MQTT connection failed with return code: {rc}'
            )

    def on_mqtt_disconnect(self, client, userdata, rc):
        self.get_logger().warn(
            f'MQTT disconnected with return code: {rc}'
        )

    def on_mqtt_message(self, client, userdata, msg):
        try:
            payload = msg.payload.decode()
            data = json.loads(payload)

            percentage = data.get('percentage', math.nan)
            voltage = data.get('voltage', math.nan)
            current = data.get('current', math.nan)

            # Simpan data terakhir dari MQTT.
            # Jangan langsung publish di sini agar ROS output tetap stabil 1 Hz.
            self.latest_percentage = self.normalize_percentage(percentage)
            self.latest_voltage = self.to_float_or_nan(voltage)
            self.latest_current = self.to_float_or_nan(current)
            self.latest_mqtt_time = time.time()

            # Kalau data baru masuk lagi setelah stale, reset warning
            self.stale_data_warned = False

            # Print hanya sekali saat data pertama berhasil diterima
            if not self.battery_data_received_logged:
                self.get_logger().info(
                    f'Battery data received from MQTT. '
                    f'Publishing to {self.ros_battery_topic} at {self.publish_rate_hz:.1f} Hz'
                )
                self.battery_data_received_logged = True

        except Exception as e:
            self.get_logger().error(
                f'Failed to parse MQTT battery message: {e}'
            )

    def publish_battery_timer(self):
        # Jangan publish apa pun sebelum MQTT battery pertama diterima.
        if self.latest_mqtt_time is None:
            return

        now = time.time()
        data_age = now - self.latest_mqtt_time

        # Kalau MQTT lama tidak update, tetap publish nilai terakhir
        # agar logger tetap dapat 1 Hz, tapi kasih warning sekali.
        if data_age > 10.0 and not self.stale_data_warned:
            self.get_logger().warning(
                f'Battery MQTT data is stale ({data_age:.1f}s old). '
                'Republishing last known battery value.'
            )
            self.stale_data_warned = True

        battery_msg = BatteryState()
        battery_msg.header.stamp = self.get_clock().now().to_msg()

        battery_msg.percentage = self.latest_percentage
        battery_msg.voltage = self.latest_voltage
        battery_msg.current = self.latest_current

        self.battery_pub.publish(battery_msg)

    def normalize_percentage(self, value):
        if value is None:
            return math.nan

        try:
            percentage = float(value)

            # Kalau input dari MQTT 0-100, ubah ke 0.0-1.0
            if percentage > 1.0:
                return percentage / 100.0

            # Kalau input sudah 0.0-1.0, langsung pakai
            return percentage

        except (TypeError, ValueError):
            return math.nan

    def to_float_or_nan(self, value):
        if value is None:
            return math.nan

        try:
            return float(value)
        except (TypeError, ValueError):
            return math.nan

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

    node.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()