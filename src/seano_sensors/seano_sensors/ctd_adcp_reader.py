#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CTD + ADCP Combined Reader - Two-Line Arduino @115200
=====================================================

Fungsi:
    Membaca satu stream serial dari ESP32/Arduino yang mengirim dua line
    back-to-back setiap 1 Hz:

        CTD:<depth_m>,<temp_c>,<cond>,<salinity_psu>,<density>,<soundvel_ms>
        ADCP:<temp_c>,<v1_ms>,<v2_ms>,<v3_ms>,<v4_ms>

Publish:
    /ctd/data   Float64MultiArray
                [depth_m, temp_c, cond, salinity_psu, density, soundvel_ms]

    /adcp/data  Float64MultiArray
                [temp_c, v1_ms, v2_ms, v3_ms, v4_ms]

Default disesuaikan dengan firmware Arduino baru:
    baud_rate = 115200

Terminal output:
    CTD_ADCP_READER: NOT READY
    CTD_ADCP_READER: READY

READY default:
    serial terbuka + CTD fresh + ADCP fresh

Catatan:
    Dua line serial tidak bisa benar-benar simultan karena dikirim berurutan.
    Tetapi dengan 115200 baud dan tanpa delay antar Serial.println(), selisih
    CTD-ADCP harus kecil.
"""

import threading
import time
from typing import List, Optional

import rclpy
from rclpy.node import Node
from std_msgs.msg import Float64MultiArray

import serial


class CTDADCPReader(Node):
    def __init__(self):
        super().__init__("ctd_adcp_reader")

        self.declare_parameter(
            "serial_port",
            "/dev/serial/by-id/usb-Silicon_Labs_CP2102_USB_to_UART_Bridge_Controller_0001-if00-port0",
        )
        self.declare_parameter("baud_rate", 57600)
        self.declare_parameter("serial_timeout_sec", 0.2)
        self.declare_parameter("inter_byte_timeout_sec", 0.05)
        self.declare_parameter("reconnect_delay_sec", 3.0)

        self.declare_parameter("ctd_topic", "/ctd/data")
        self.declare_parameter("adcp_topic", "/adcp/data")

        self.declare_parameter("ready_timeout_sec", 3.0)
        self.declare_parameter("require_both_streams", True)

        self.declare_parameter("verbose_errors", False)
        self.declare_parameter("warn_every_bad_line", 50)

        self.serial_port = str(self.get_parameter("serial_port").value)
        self.baud_rate = int(self.get_parameter("baud_rate").value)
        self.serial_timeout_sec = float(self.get_parameter("serial_timeout_sec").value)
        self.inter_byte_timeout_sec = float(self.get_parameter("inter_byte_timeout_sec").value)
        self.reconnect_delay_sec = float(self.get_parameter("reconnect_delay_sec").value)
        self.ctd_topic = str(self.get_parameter("ctd_topic").value)
        self.adcp_topic = str(self.get_parameter("adcp_topic").value)
        self.ready_timeout_sec = float(self.get_parameter("ready_timeout_sec").value)
        self.require_both_streams = bool(self.get_parameter("require_both_streams").value)
        self.verbose_errors = bool(self.get_parameter("verbose_errors").value)
        self.warn_every_bad_line = max(1, int(self.get_parameter("warn_every_bad_line").value))

        if self.ready_timeout_sec <= 0.0:
            self.ready_timeout_sec = 3.0

        if self.serial_timeout_sec <= 0.0:
            self.serial_timeout_sec = 0.2

        if self.inter_byte_timeout_sec <= 0.0:
            self.inter_byte_timeout_sec = 0.05

        self.ctd_pub = self.create_publisher(Float64MultiArray, self.ctd_topic, 10)
        self.adcp_pub = self.create_publisher(Float64MultiArray, self.adcp_topic, 10)

        self.ser: Optional[serial.Serial] = None
        self.stop_event = threading.Event()

        self.serial_connected = False
        self.last_valid_ctd_time: Optional[float] = None
        self.last_valid_adcp_time: Optional[float] = None
        self.last_valid_any_time: Optional[float] = None

        self.bad_line_count = 0
        self.ctd_count = 0
        self.adcp_count = 0

        self.last_status: Optional[str] = None
        self.set_ready(False)

        self.create_timer(1.0, self.check_ready_timeout)

        self.reader_thread = threading.Thread(target=self.read_loop, daemon=True)
        self.reader_thread.start()

    def set_ready(self, ready: bool):
        status = "READY" if ready else "NOT READY"
        if status == self.last_status:
            return
        self.last_status = status
        self.get_logger().info(f"CTD_ADCP_READER: {status}")

    def is_fresh(self, timestamp: Optional[float], now: Optional[float] = None) -> bool:
        if timestamp is None:
            return False
        if now is None:
            now = time.time()
        return (now - timestamp) <= self.ready_timeout_sec

    def update_ready_state(self):
        if not self.serial_connected:
            self.set_ready(False)
            return

        now = time.time()

        if self.require_both_streams:
            ready = (
                self.is_fresh(self.last_valid_ctd_time, now)
                and self.is_fresh(self.last_valid_adcp_time, now)
            )
        else:
            ready = self.is_fresh(self.last_valid_any_time, now)

        self.set_ready(ready)

    def check_ready_timeout(self):
        self.update_ready_state()

    def open_serial(self) -> bool:
        while rclpy.ok() and not self.stop_event.is_set():
            try:
                self.close_serial(update_status=False)

                self.ser = serial.Serial(
                    port=self.serial_port,
                    baudrate=self.baud_rate,
                    timeout=self.serial_timeout_sec,
                    inter_byte_timeout=self.inter_byte_timeout_sec,
                )

                try:
                    self.ser.reset_input_buffer()
                    self.ser.reset_output_buffer()
                except Exception:
                    pass

                self.serial_connected = True
                self.last_valid_ctd_time = None
                self.last_valid_adcp_time = None
                self.last_valid_any_time = None
                self.update_ready_state()
                return True

            except serial.SerialException:
                self.serial_connected = False
                self.update_ready_state()
                time.sleep(self.reconnect_delay_sec)

            except Exception:
                self.serial_connected = False
                self.update_ready_state()
                time.sleep(self.reconnect_delay_sec)

        return False

    def close_serial(self, update_status: bool = True):
        if self.ser is not None:
            try:
                self.ser.close()
            except Exception:
                pass
            self.ser = None

        self.serial_connected = False

        if update_status:
            self.update_ready_state()

    def read_loop(self):
        if not self.open_serial():
            return

        while rclpy.ok() and not self.stop_event.is_set():
            try:
                if self.ser is None or not self.ser.is_open:
                    if not self.open_serial():
                        return

                raw = self.ser.readline()
                if not raw:
                    continue

                line = raw.decode("ascii", errors="ignore").strip()
                if not line:
                    continue

                if line.startswith("#"):
                    continue

                self.handle_line(line)

            except serial.SerialException:
                self.serial_connected = False
                self.update_ready_state()
                time.sleep(self.reconnect_delay_sec)
                self.open_serial()

            except Exception:
                self.serial_connected = False
                self.update_ready_state()
                time.sleep(0.2)
                self.open_serial()

    def handle_line(self, line: str):
        if line.startswith("CTD:"):
            self.parse_ctd(line[4:])
            return

        if line.startswith("ADCP:"):
            self.parse_adcp(line[5:])
            return

        ctd_index = line.find("CTD:")
        adcp_index = line.find("ADCP:")

        if ctd_index >= 0 and (adcp_index < 0 or ctd_index < adcp_index):
            self.parse_ctd(line[ctd_index + 4:])
            return

        if adcp_index >= 0:
            self.parse_adcp(line[adcp_index + 5:])
            return

        self.report_bad_line()

    def parse_float_list(self, payload: str, expected_len: int) -> Optional[List[float]]:
        parts = [part.strip() for part in payload.split(",")]

        if len(parts) < expected_len:
            self.report_bad_line()
            return None

        try:
            return [float(value) for value in parts[:expected_len]]
        except ValueError:
            self.report_bad_line()
            return None

    def parse_ctd(self, payload: str):
        values = self.parse_float_list(payload, 6)
        if values is None:
            return

        msg = Float64MultiArray()
        msg.data = values
        self.ctd_pub.publish(msg)

        now = time.time()
        self.ctd_count += 1
        self.last_valid_ctd_time = now
        self.last_valid_any_time = now
        self.update_ready_state()

    def parse_adcp(self, payload: str):
        values = self.parse_float_list(payload, 5)
        if values is None:
            return

        msg = Float64MultiArray()
        msg.data = values
        self.adcp_pub.publish(msg)

        now = time.time()
        self.adcp_count += 1
        self.last_valid_adcp_time = now
        self.last_valid_any_time = now
        self.update_ready_state()

    def report_bad_line(self):
        self.bad_line_count += 1

        if not self.verbose_errors:
            return

        if self.bad_line_count == 1 or self.bad_line_count % self.warn_every_bad_line == 0:
            self.get_logger().warning(
                f"CTD_ADCP_READER: bad serial line count={self.bad_line_count}"
            )

    def destroy_node(self):
        self.stop_event.set()

        try:
            if hasattr(self, "reader_thread") and self.reader_thread.is_alive():
                self.reader_thread.join(timeout=1.0)
        except Exception:
            pass

        self.close_serial()
        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)
    node = CTDADCPReader()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()