#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
from datetime import datetime

import cv2
import numpy as np
from cv_bridge import CvBridge

import rclpy
from rclpy.node import Node
from rclpy.qos import (
    QoSProfile,
    ReliabilityPolicy,
    HistoryPolicy,
    DurabilityPolicy,
)

from sensor_msgs.msg import Image, CompressedImage
from mavros_msgs.msg import State


class SeanoVideoLogger(Node):
    def __init__(self):
        # JANGAN DIUBAH: node name tetap sama agar launch/service lama tetap jalan
        super().__init__("video_logger_node")

        # Input video. Default compressed karena kamu mau yang HUD / bounding box,
        # bukan raw camera.
        self.declare_parameter("image_topic", "/seano/camera/debug/compressed")
        self.declare_parameter("image_type", "compressed")  # "compressed" atau "raw"

        # Parameter lama tetap dideklarasikan untuk kompatibilitas,
        # tapi untuk compressed topic reliability praktis pakai BEST_EFFORT.
        self.declare_parameter("image_reliability", "best_effort")

        # Storage
        self.declare_parameter("external_mount_point", "/mnt/seano/SEANO_SSD")
        self.declare_parameter("local_mount_point", os.path.expanduser("~/Documents/SEANO_logs"))

        self.declare_parameter("enable_external_logging", True)
        self.declare_parameter("enable_local_logging", True)
        self.declare_parameter("require_external_on_mission", False)

        # Video config
        self.declare_parameter("output_fps", 30.0)
        self.declare_parameter("codec", "mp4v")
        self.declare_parameter("segment_seconds", 60.0)
        self.declare_parameter("record_every_n_frames", 1)

        # Mission gate
        self.declare_parameter("mission_gate_topic", "/mavros/state")

        # Tetap dideklarasikan supaya launch lama tidak error kalau masih mengirim param ini.
        # Tapi sengaja tidak dipakai untuk start logging. Logging tetap wajib MAVROS armed=True.
        self.declare_parameter("force_record_without_mavros", False)

        self.image_topic = str(self.get_parameter("image_topic").value)
        self.image_type = str(self.get_parameter("image_type").value).strip().lower()
        self.image_reliability = str(self.get_parameter("image_reliability").value).strip().lower()

        self.external_mount_point = os.path.expanduser(
            str(self.get_parameter("external_mount_point").value)
        )
        self.local_mount_point = os.path.expanduser(
            str(self.get_parameter("local_mount_point").value)
        )

        self.enable_external_logging = bool(self.get_parameter("enable_external_logging").value)
        self.enable_local_logging = bool(self.get_parameter("enable_local_logging").value)
        self.require_external_on_mission = bool(
            self.get_parameter("require_external_on_mission").value
        )

        self.output_fps = float(self.get_parameter("output_fps").value)
        self.codec = str(self.get_parameter("codec").value)
        self.segment_seconds = float(self.get_parameter("segment_seconds").value)
        self.record_every_n_frames = max(
            1,
            int(self.get_parameter("record_every_n_frames").value),
        )

        self.mission_gate_topic = str(self.get_parameter("mission_gate_topic").value)
        self.force_record_without_mavros = bool(
            self.get_parameter("force_record_without_mavros").value
        )

        if self.force_record_without_mavros:
            self.get_logger().warning(
                "force_record_without_mavros=True terdeteksi, tapi di versi ini DIABAIKAN. "
                "Video logging tetap hanya mulai saat MAVROS armed=True."
            )

        if self.image_type not in ["compressed", "raw"]:
            self.get_logger().warning(
                f"Invalid image_type='{self.image_type}', fallback to compressed"
            )
            self.image_type = "compressed"

        if self.output_fps <= 0.0:
            self.get_logger().warning("output_fps <= 0, fallback to 30.0 FPS")
            self.output_fps = 30.0

        if len(self.codec) != 4:
            self.get_logger().warning(
                f"Invalid codec '{self.codec}', fallback to mp4v"
            )
            self.codec = "mp4v"

        self.bridge = CvBridge()

        self.logging_active = False

        self.last_armed_state = False
        self.last_connected_state = False
        self.last_flight_mode = "UNKNOWN"
        self.state_received = False

        self.start_time_obj = None
        self.local_timezone = time.tzname[0]
        self.mission_id = None

        self.targets = []

        self.segment_index = 0
        self.segment_start_wall = 0.0

        self.frame_width = None
        self.frame_height = None

        self.frame_count_in = 0
        self.frame_count_written = 0

        self.last_rate_log_time = time.time()
        self.last_rate_log_frame_count = 0

        self.state_sub = self.create_subscription(
            State,
            self.mission_gate_topic,
            self.mavros_state_callback,
            self.make_state_qos(),
        )

        if self.image_type == "compressed":
            self.image_sub = self.create_subscription(
                CompressedImage,
                self.image_topic,
                self.compressed_image_callback,
                self.make_image_qos(),
            )
        else:
            self.image_sub = self.create_subscription(
                Image,
                self.image_topic,
                self.raw_image_callback,
                self.make_image_qos(),
            )

        self.watchdog_timer = self.create_timer(5.0, self.watchdog_callback)

        self.get_logger().info("SEANO Video Logger standby")
        self.get_logger().info(
            f"Input topic={self.image_topic}, image_type={self.image_type}, "
            f"reliability={self.image_reliability}"
        )
        self.get_logger().info(
            f"Mission gate aktif: logging hanya saat {self.mission_gate_topic} armed=True"
        )
        self.get_logger().info(
            f"Segment duration={self.segment_seconds:.1f}s, output_fps={self.output_fps:.2f}"
        )

    def make_state_qos(self):
        return QoSProfile(
            history=HistoryPolicy.KEEP_LAST,
            depth=10,
            reliability=ReliabilityPolicy.BEST_EFFORT,
            durability=DurabilityPolicy.VOLATILE,
        )

    def make_image_qos(self):
        if self.image_reliability == "reliable":
            reliability = ReliabilityPolicy.RELIABLE
        else:
            reliability = ReliabilityPolicy.BEST_EFFORT

        return QoSProfile(
            history=HistoryPolicy.KEEP_LAST,
            depth=5,
            reliability=reliability,
            durability=DurabilityPolicy.VOLATILE,
        )

    def mavros_state_callback(self, msg):
        self.state_received = True

        self.last_connected_state = msg.connected
        self.last_armed_state = msg.armed
        self.last_flight_mode = msg.mode

        if msg.armed and not self.logging_active:
            self.start_logging_session(msg)

        elif not msg.armed and self.logging_active:
            reason = (
                f"mode={msg.mode}, "
                f"armed={msg.armed}, "
                f"connected={msg.connected}"
            )
            self.stop_logging_session(reason)

    def watchdog_callback(self):
        if not self.state_received:
            self.get_logger().warning(
                f"No MAVROS state received yet from {self.mission_gate_topic}. "
                "Video logging belum bisa mulai."
            )

        self.get_logger().info(
            f"Video logger status | "
            f"state_received={self.state_received}, "
            f"armed={self.last_armed_state}, "
            f"connected={self.last_connected_state}, "
            f"mode={self.last_flight_mode}, "
            f"logging_active={self.logging_active}, "
            f"frames_in={self.frame_count_in}, "
            f"frames_written={self.frame_count_written}"
        )

    def compressed_image_callback(self, msg):
        self.frame_count_in += 1

        if self.frame_count_in % self.record_every_n_frames != 0:
            return

        np_arr = np.frombuffer(msg.data, np.uint8)
        frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

        if frame is None:
            self.get_logger().warning("Failed decoding compressed image")
            return

        self.process_frame(frame)

    def raw_image_callback(self, msg):
        self.frame_count_in += 1

        if self.frame_count_in % self.record_every_n_frames != 0:
            return

        try:
            frame = self.bridge.imgmsg_to_cv2(msg, desired_encoding="bgr8")
        except Exception as e:
            self.get_logger().error(f"Failed converting raw image to OpenCV: {e}")
            return

        if frame is None:
            return

        self.process_frame(frame)

    def process_frame(self, frame):
        height, width = frame.shape[:2]

        if self.logging_active:
            self.write_video_frame(frame, width, height)

        self.log_input_rate()

    def start_logging_session(self, state_msg=None):
        if self.logging_active:
            return

        self.get_logger().info("ARMED detected -> preparing video logger session")

        self.start_time_obj = datetime.now()
        self.local_timezone = time.tzname[0]

        year = self.start_time_obj.strftime("%Y")
        month = self.start_time_obj.strftime("%m")
        day = self.start_time_obj.strftime("%d")

        self.mission_id = self.start_time_obj.strftime(
            f"MISSION_START_%H-%M-%S_{self.local_timezone}"
        )

        self.targets = []
        self.segment_index = 0
        self.segment_start_wall = 0.0

        self.frame_width = None
        self.frame_height = None

        self.frame_count_written = 0

        if self.enable_external_logging:
            external_base_path = os.path.join(
                self.external_mount_point,
                "SEANO_MISSIONS",
                year,
                month,
                day,
                self.mission_id,
            )

            if self.is_path_writable(self.external_mount_point):
                try:
                    os.makedirs(external_base_path, exist_ok=True)

                    if not self.test_write_access(external_base_path):
                        raise RuntimeError("External SSD detected but not writable")

                    self.add_logging_target(external_base_path, "external")
                    self.get_logger().info(
                        f"External video logging ready: {external_base_path}"
                    )

                except Exception as e:
                    self.get_logger().error(
                        f"Gagal menyiapkan external video logging: {e}"
                    )
            else:
                self.get_logger().warning(
                    f"SSD external belum siap / tidak writable: {self.external_mount_point}"
                )

        if self.require_external_on_mission and len(self.targets) == 0:
            self.get_logger().fatal(
                "Armed terdeteksi, tetapi external SSD belum siap. "
                "Video logger tetap standby."
            )
            return

        if self.enable_local_logging:
            local_base_path = os.path.join(
                self.local_mount_point,
                year,
                month,
                day,
                self.mission_id,
            )

            try:
                os.makedirs(local_base_path, exist_ok=True)

                if not self.test_write_access(local_base_path):
                    raise RuntimeError("Local path not writable")

                self.add_logging_target(local_base_path, "local")
                self.get_logger().info(
                    f"Local video logging ready: {local_base_path}"
                )

            except Exception as e:
                self.get_logger().error(
                    f"Gagal menyiapkan local video logging: {e}"
                )

        if len(self.targets) == 0:
            self.get_logger().fatal("Tidak ada path video logging yang valid")
            return

        for target in self.targets:
            self.write_start_info(target, state_msg)

        self.logging_active = True

        self.get_logger().info(
            f"VIDEO LOGGER ACTIVE | topic={self.image_topic} | "
            f"image_type={self.image_type} | "
            f"mode={self.last_flight_mode}, "
            f"armed={self.last_armed_state}, "
            f"connected={self.last_connected_state}"
        )

    def add_logging_target(self, base_path, target_name):
        video_dir = os.path.join(base_path, "video")
        os.makedirs(video_dir, exist_ok=True)

        index_path = os.path.join(video_dir, "video_frames.csv")
        index_file = open(index_path, "w")

        index_file.write(
            "local_timestamp,"
            "segment_index,"
            "frame_index,"
            "width,"
            "height,"
            "filename\n"
        )
        index_file.flush()

        target = {
            "name": target_name,
            "base_path": base_path,
            "video_dir": video_dir,
            "index_path": index_path,
            "index_file": index_file,
            "writer": None,
            "temp_path": None,
            "final_path": None,
            "current_filename": None,
        }

        self.targets.append(target)

    def write_start_info(self, target, state_msg=None):
        info_path = os.path.join(target["video_dir"], "video_info.txt")

        with open(info_path, "w") as f:
            f.write(f"Start Time: {self.start_time_obj}\n")
            f.write(f"Timezone: {self.local_timezone}\n")
            f.write("Platform: SEANO USV\n")
            f.write("Logger Type: HUD / Bounding Box Video Logger\n")
            f.write(f"Target Name: {target['name']}\n")
            f.write(f"Base Path: {target['base_path']}\n")
            f.write(f"Image Topic: {self.image_topic}\n")
            f.write(f"Image Type: {self.image_type}\n")
            f.write(f"Image Reliability: {self.image_reliability}\n")
            f.write(f"Output FPS: {self.output_fps}\n")
            f.write(f"Codec: {self.codec}\n")
            f.write(f"Segment Seconds: {self.segment_seconds}\n")
            f.write(f"Record Every N Frames: {self.record_every_n_frames}\n")

            if state_msg is not None:
                f.write(f"Start MAVROS Connected: {state_msg.connected}\n")
                f.write(f"Start MAVROS Armed: {state_msg.armed}\n")
                f.write(f"Start MAVROS Mode: {state_msg.mode}\n")
            else:
                f.write("Start MAVROS Connected: UNKNOWN\n")
                f.write("Start MAVROS Armed: UNKNOWN\n")
                f.write("Start MAVROS Mode: UNKNOWN\n")

    def write_video_frame(self, frame, width, height):
        if self.frame_width is None or self.frame_height is None:
            self.frame_width = width
            self.frame_height = height
            self.open_new_segment(width, height)

        if width != self.frame_width or height != self.frame_height:
            self.get_logger().warning(
                f"Frame size changed from "
                f"{self.frame_width}x{self.frame_height} to {width}x{height}; "
                f"resizing to original writer size"
            )

            frame = cv2.resize(
                frame,
                (self.frame_width, self.frame_height),
                interpolation=cv2.INTER_AREA,
            )

            width = self.frame_width
            height = self.frame_height

        now = time.time()

        if (
            self.segment_seconds > 0.0
            and self.segment_start_wall > 0.0
            and now - self.segment_start_wall >= self.segment_seconds
        ):
            self.open_new_segment(width, height)

        active_writer_count = 0

        for target in self.targets:
            writer = target["writer"]

            if writer is None:
                continue

            try:
                writer.write(frame)
                active_writer_count += 1
            except Exception as e:
                self.get_logger().error(
                    f"Video writer failed for {target['name']}: {e}"
                )

        if active_writer_count == 0:
            self.get_logger().error("No active video writer available")
            return

        self.frame_count_written += 1

        local_timestamp = self.get_local_timestamp()

        for target in self.targets:
            index_file = target["index_file"]

            if index_file is None:
                continue

            try:
                filename = target["current_filename"]

                if filename is None:
                    filename = ""

                index_file.write(
                    f"{local_timestamp},"
                    f"{self.segment_index},"
                    f"{self.frame_count_written},"
                    f"{width},"
                    f"{height},"
                    f"{filename}\n"
                )
                index_file.flush()

            except Exception as e:
                self.get_logger().error(
                    f"Failed writing video index for {target['name']}: {e}"
                )

    def open_new_segment(self, width, height):
        self.close_current_segment()

        self.segment_index += 1
        self.segment_start_wall = time.time()

        final_filename = f"video_segment_{self.segment_index:03d}.mp4"
        temp_filename = f"video_segment_{self.segment_index:03d}.recording.mp4"

        fourcc = cv2.VideoWriter_fourcc(*self.codec)

        opened_count = 0

        for target in self.targets:
            video_dir = target["video_dir"]

            temp_path = os.path.join(video_dir, temp_filename)
            final_path = os.path.join(video_dir, final_filename)

            target["writer"] = None
            target["temp_path"] = temp_path
            target["final_path"] = final_path
            target["current_filename"] = final_filename

            try:
                writer = cv2.VideoWriter(
                    temp_path,
                    fourcc,
                    self.output_fps,
                    (int(width), int(height)),
                )

                if not writer.isOpened():
                    self.get_logger().error(
                        f"Failed opening VideoWriter for {target['name']}: {temp_path}"
                    )
                    target["writer"] = None
                    continue

                target["writer"] = writer
                opened_count += 1

                self.get_logger().info(
                    f"Video segment opened [{target['name']}]: "
                    f"{temp_path} | {width}x{height} @ {self.output_fps:.2f} FPS"
                )

            except Exception as e:
                self.get_logger().error(
                    f"Exception opening VideoWriter for {target['name']}: {e}"
                )
                target["writer"] = None

        if opened_count == 0:
            self.get_logger().error("No VideoWriter could be opened for this segment")

    def close_current_segment(self):
        for target in self.targets:
            writer = target.get("writer")
            temp_path = target.get("temp_path")
            final_path = target.get("final_path")

            if writer is not None:
                try:
                    writer.release()
                except Exception as e:
                    self.get_logger().error(
                        f"Failed releasing writer for {target['name']}: {e}"
                    )

                target["writer"] = None

            if temp_path is not None and final_path is not None:
                try:
                    if os.path.exists(temp_path):
                        os.replace(temp_path, final_path)
                        self.get_logger().info(
                            f"Video segment finalized [{target['name']}]: {final_path}"
                        )
                except Exception as e:
                    self.get_logger().error(
                        f"Failed finalizing video segment for {target['name']}: "
                        f"{temp_path} -> {final_path} | {e}"
                    )

            target["temp_path"] = None
            target["final_path"] = None
            target["current_filename"] = None

        try:
            os.sync()
        except AttributeError:
            pass

    def stop_logging_session(self, reason="vehicle disarmed"):
        if not self.logging_active:
            return

        self.logging_active = False

        self.get_logger().info(
            f"Stopping video logger session | reason={reason}"
        )

        self.close_current_segment()

        end_time_obj = datetime.now()

        for target in self.targets:
            try:
                info_path = os.path.join(target["video_dir"], "video_info.txt")

                with open(info_path, "a") as f:
                    f.write(f"End Time: {end_time_obj}\n")
                    f.write(f"Stop Reason: {reason}\n")
                    f.write(f"Total Input Frames: {self.frame_count_in}\n")
                    f.write(f"Total Written Frames: {self.frame_count_written}\n")
                    f.write(f"End MAVROS Connected: {self.last_connected_state}\n")
                    f.write(f"End MAVROS Armed: {self.last_armed_state}\n")
                    f.write(f"End MAVROS Mode: {self.last_flight_mode}\n")

            except Exception as e:
                self.get_logger().error(
                    f"Failed writing video end info for {target['name']}: {e}"
                )

            try:
                if target["index_file"] is not None:
                    target["index_file"].flush()
                    target["index_file"].close()
            except Exception:
                pass

            target["index_file"] = None

        try:
            os.sync()
        except AttributeError:
            pass

        self.targets = []
        self.segment_index = 0
        self.segment_start_wall = 0.0

        self.frame_width = None
        self.frame_height = None

        self.get_logger().info(
            "SEANO Video Logger kembali standby, menunggu armed berikutnya"
        )

    def log_input_rate(self):
        now = time.time()
        elapsed = now - self.last_rate_log_time

        if elapsed >= 5.0:
            frame_delta = self.frame_count_in - self.last_rate_log_frame_count
            measured_fps = frame_delta / elapsed

            self.get_logger().info(
                f"Input image rate={measured_fps:.2f} FPS | "
                f"frames_in={self.frame_count_in} | "
                f"frames_written={self.frame_count_written} | "
                f"logging_active={self.logging_active}"
            )

            self.last_rate_log_time = now
            self.last_rate_log_frame_count = self.frame_count_in

    def get_local_timestamp(self):
        now = datetime.now()
        return now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def is_path_writable(self, path):
        return os.path.exists(path) and os.access(path, os.W_OK)

    def test_write_access(self, path):
        test_file = os.path.join(path, ".seano_video_write_test")

        try:
            with open(test_file, "w") as f:
                f.write("ok")

            os.remove(test_file)
            return True

        except Exception:
            return False

    def destroy_node(self):
        if self.logging_active:
            self.stop_logging_session("node shutdown")
        else:
            self.close_current_segment()

            for target in self.targets:
                try:
                    if target["index_file"] is not None:
                        target["index_file"].flush()
                        target["index_file"].close()
                except Exception:
                    pass

        try:
            os.sync()
        except AttributeError:
            pass

        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)

    node = SeanoVideoLogger()

    try:
        rclpy.spin(node)

    except KeyboardInterrupt:
        pass

    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()