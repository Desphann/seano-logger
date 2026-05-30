#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CTD Reader - Serial USB
=======================
Membaca data raw CTD dari ESP32 via Serial USB dan publish ke ROS2.

Format serial masuk:
    CTD:<depth>,<temp>,<cond>,<salinity>,<density>,<soundvel>

Publish ke /ctd/data  (Float64MultiArray):
    [depth, temp, cond, salinity, density, soundvel]
"""

import rclpy
from rclpy.node import Node
from std_msgs.msg import Float64MultiArray

import serial
import threading


class CTDReader(Node):
    def __init__(self):
        super().__init__('ctd_reader')

        # ── Parameter ──────────────────────────────────────────────
        self.declare_parameter('serial_port', '/dev/ttyUSB0')
        self.declare_parameter('baud_rate', 115200)

        self.serial_port = self.get_parameter('serial_port').value
        self.baud_rate   = int(self.get_parameter('baud_rate').value)

        # ── Publisher ──────────────────────────────────────────────
        self.publisher_ = self.create_publisher(Float64MultiArray, '/ctd/data', 10)

        # ── State ──────────────────────────────────────────────────
        self.publish_ok_reported    = False
        self.publish_error_reported = False
        self.ser                    = None

        self.get_logger().info(
            f"CTD Reader started | port={self.serial_port} | baud={self.baud_rate} | topic=/ctd/data"
        )

        # Serial dibaca di thread terpisah agar tidak block spin ROS2
        self._thread = threading.Thread(target=self._read_loop, daemon=True)
        self._thread.start()

    # ──────────────────────────────────────────────────────────────
    #  Serial read loop
    # ──────────────────────────────────────────────────────────────
    def _open_serial(self):
        while rclpy.ok():
            try:
                self.ser = serial.Serial(
                    self.serial_port,
                    self.baud_rate,
                    timeout=2.0
                )
                self.get_logger().info(f"Serial port terbuka: {self.serial_port}")
                return True
            except serial.SerialException as e:
                self.get_logger().error(
                    f"Gagal membuka serial {self.serial_port}: {e} | retry 3s..."
                )
                import time; time.sleep(3.0)
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

                # Abaikan baris komentar / header dari ESP32
                if not line or line.startswith('#'):
                    continue

                # Hanya proses baris CTD
                if not line.startswith('CTD:'):
                    continue

                self._parse_and_publish(line[4:])   # buang prefix "CTD:"

            except serial.SerialException as e:
                self.get_logger().error(f"Serial error: {e} | mencoba reconnect...")
                self._open_serial()
            except Exception as e:
                if not self.publish_error_reported:
                    self.get_logger().error(f"CTD parse error: {e}")
                    self.publish_error_reported = True
                    self.publish_ok_reported    = False

    # ──────────────────────────────────────────────────────────────
    #  Parse & publish
    # ──────────────────────────────────────────────────────────────
    def _parse_and_publish(self, payload: str):
        """
        payload = "depth,temp,cond,salinity,density,soundvel"
        """
        parts = payload.split(',')
        if len(parts) < 6:
            self.get_logger().warn(f"CTD: format tidak lengkap -> '{payload}'")
            return

        try:
            depth    = float(parts[0])
            temp     = float(parts[1])
            cond     = float(parts[2])
            salinity = float(parts[3])
            density  = float(parts[4])
            soundvel = float(parts[5])
        except ValueError as e:
            self.get_logger().warn(f"CTD: konversi float gagal -> {e}")
            return

        msg      = Float64MultiArray()
        msg.data = [depth, temp, cond, salinity, density, soundvel]
        self.publisher_.publish(msg)

        if not self.publish_ok_reported:
            self.get_logger().info(
                f"CTD publish aktif | depth={depth:.3f} temp={temp:.3f} "
                f"cond={cond:.3f} sal={salinity:.3f} den={density:.3f} sv={soundvel:.3f}"
            )
            self.publish_ok_reported    = True
            self.publish_error_reported = False


def main(args=None):
    rclpy.init(args=args)
    node = CTDReader()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()