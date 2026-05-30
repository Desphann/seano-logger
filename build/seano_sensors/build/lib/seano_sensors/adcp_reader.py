#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ADCP Reader - Serial USB
========================
Membaca data raw ADCP dari ESP32 via Serial USB dan publish ke ROS2.

Format serial masuk:
    ADCP:<Temp_C>,<V1_ms>,<V2_ms>,<V3_ms>,<V4_ms>

    Temp_C : 24.86 ~ 24.98  °C
    V1_ms  : -0.213 ~ -0.003 m/s
    V2_ms  :  0.058 ~  0.158 m/s
    V3_ms  : -0.019 ~  0.001 m/s
    V4_ms  : -0.058 ~ -0.012 m/s

Publish ke /adcp/data  (Float64MultiArray):
    [Temp_C, V1_ms, V2_ms, V3_ms, V4_ms]
"""

import threading

import rclpy
from rclpy.node import Node
from std_msgs.msg import Float64MultiArray

import serial


class ADCPReader(Node):
    def __init__(self):
        super().__init__('adcp_reader')

        # ── Parameter ──────────────────────────────────────────────
        self.declare_parameter('serial_port', '/dev/ttyUSB0')
        self.declare_parameter('baud_rate',   115200)

        self.serial_port = self.get_parameter('serial_port').value
        self.baud_rate   = int(self.get_parameter('baud_rate').value)

        # ── Publisher ──────────────────────────────────────────────
        self.publisher_ = self.create_publisher(Float64MultiArray, '/adcp/data', 10)

        # ── State ──────────────────────────────────────────────────
        self.publish_ok_reported    = False
        self.publish_error_reported = False
        self.ser                    = None

        self.get_logger().info(
            f"ADCP Reader started | port={self.serial_port} | "
            f"baud={self.baud_rate} | topic=/adcp/data"
        )

        self._thread = threading.Thread(target=self._read_loop, daemon=True)
        self._thread.start()

    # ──────────────────────────────────────────────────────────────
    #  Serial
    # ──────────────────────────────────────────────────────────────
    def _open_serial(self):
        import time
        while rclpy.ok():
            try:
                self.ser = serial.Serial(self.serial_port, self.baud_rate, timeout=2.0)
                self.get_logger().info(f"Serial port terbuka: {self.serial_port}")
                return True
            except serial.SerialException as e:
                self.get_logger().error(f"Gagal buka serial {self.serial_port}: {e} | retry 3s...")
                time.sleep(3.0)
        return False

    def _read_loop(self):
        if not self._open_serial():
            return

        while rclpy.ok():
            try:
                raw = self.ser.readline()
                if not raw:
                    continue

                line = raw.decode('ascii', errors='ignore').strip()

                if not line or line.startswith('#'):
                    continue

                if not line.startswith('ADCP:'):
                    continue

                self._parse_and_publish(line[5:])   # buang prefix "ADCP:"

            except serial.SerialException as e:
                self.get_logger().error(f"Serial error: {e} | reconnect...")
                self._open_serial()
            except Exception as e:
                if not self.publish_error_reported:
                    self.get_logger().error(f"ADCP parse error: {e}")
                    self.publish_error_reported = True
                    self.publish_ok_reported    = False

    # ──────────────────────────────────────────────────────────────
    #  Parse & publish
    # ──────────────────────────────────────────────────────────────
    def _parse_and_publish(self, payload: str):
        """
        payload = "Temp_C,V1_ms,V2_ms,V3_ms,V4_ms"
        """
        parts = payload.split(',')
        if len(parts) < 5:
            self.get_logger().warn(f"ADCP: format tidak lengkap -> '{payload}'")
            return

        try:
            temp_c = float(parts[0])
            v1     = float(parts[1])
            v2     = float(parts[2])
            v3     = float(parts[3])
            v4     = float(parts[4])
        except ValueError as e:
            self.get_logger().warn(f"ADCP: konversi float gagal -> {e}")
            return

        msg      = Float64MultiArray()
        msg.data = [temp_c, v1, v2, v3, v4]
        self.publisher_.publish(msg)

        if not self.publish_ok_reported:
            self.get_logger().info(
                f"ADCP publish aktif | temp={temp_c:.2f}°C | "
                f"V1={v1:.3f} V2={v2:.3f} V3={v3:.3f} V4={v4:.3f} m/s"
            )
            self.publish_ok_reported    = True
            self.publish_error_reported = False


def main(args=None):
    rclpy.init(args=args)
    node = ADCPReader()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()