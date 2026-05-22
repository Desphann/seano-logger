#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
from datetime import datetime

import rclpy
from rclpy.node import Node
from rclpy.qos import (
    QoSProfile,
    ReliabilityPolicy,
    HistoryPolicy,
    DurabilityPolicy,
)

from std_msgs.msg import String
from sensor_msgs.msg import NavSatFix
from mavros_msgs.msg import (
    State,
    WaypointList,
    WaypointReached,
    VfrHud,
    StatusText,
)


class MissionStateReaderNode(Node):
    def __init__(self):
        super().__init__("mission_state_reader_node")

        self.declare_parameter("state_topic", "/mavros/state")
        self.declare_parameter("waypoints_topic", "/mavros/mission/waypoints")
        self.declare_parameter("waypoint_reached_topic", "/mavros/mission/reached")
        self.declare_parameter("vfr_hud_topic", "/mavros/vfr_hud")
        self.declare_parameter("gps_topic", "/mavros/global_position/raw/fix")
        self.declare_parameter("statustext_topic", "/mavros/statustext/recv")

        self.declare_parameter("snapshot_topic", "/seano/mission/snapshot")
        self.declare_parameter("event_topic", "/seano/mission/event")
        self.declare_parameter("waypoints_out_topic", "/seano/mission/waypoints")

        self.declare_parameter("snapshot_rate_hz", 1.0)

        self.state_topic = str(self.get_parameter("state_topic").value)
        self.waypoints_topic = str(self.get_parameter("waypoints_topic").value)
        self.waypoint_reached_topic = str(
            self.get_parameter("waypoint_reached_topic").value
        )
        self.vfr_hud_topic = str(self.get_parameter("vfr_hud_topic").value)
        self.gps_topic = str(self.get_parameter("gps_topic").value)
        self.statustext_topic = str(self.get_parameter("statustext_topic").value)

        self.snapshot_topic = str(self.get_parameter("snapshot_topic").value)
        self.event_topic = str(self.get_parameter("event_topic").value)
        self.waypoints_out_topic = str(
            self.get_parameter("waypoints_out_topic").value
        )

        self.snapshot_rate_hz = float(self.get_parameter("snapshot_rate_hz").value)
        if self.snapshot_rate_hz <= 0.0:
            self.snapshot_rate_hz = 1.0

        self.qos = QoSProfile(
            history=HistoryPolicy.KEEP_LAST,
            depth=20,
            reliability=ReliabilityPolicy.BEST_EFFORT,
            durability=DurabilityPolicy.VOLATILE,
        )

        self.snapshot_pub = self.create_publisher(String, self.snapshot_topic, 10)
        self.event_pub = self.create_publisher(String, self.event_topic, 50)
        self.waypoints_pub = self.create_publisher(String, self.waypoints_out_topic, 10)

        self.create_subscription(State, self.state_topic, self.state_callback, self.qos)
        self.create_subscription(
            WaypointList,
            self.waypoints_topic,
            self.waypoints_callback,
            self.qos,
        )
        self.create_subscription(
            WaypointReached,
            self.waypoint_reached_topic,
            self.waypoint_reached_callback,
            self.qos,
        )
        self.create_subscription(VfrHud, self.vfr_hud_topic, self.vfr_hud_callback, self.qos)
        self.create_subscription(NavSatFix, self.gps_topic, self.gps_callback, self.qos)
        self.create_subscription(
            StatusText,
            self.statustext_topic,
            self.statustext_callback,
            self.qos,
        )

        self.timer = self.create_timer(
            1.0 / self.snapshot_rate_hz,
            self.publish_snapshot,
        )

        self.last_connected = False
        self.last_armed = False
        self.last_guided = False
        self.last_manual_input = False
        self.last_mode = "UNKNOWN"
        self.last_mode_class = "OTHER"
        self.last_system_status = 0

        self.previous_armed = False
        self.previous_mode = "UNKNOWN"
        self.previous_mode_class = "OTHER"

        self.current_waypoint_seq = -1
        self.last_reached_waypoint_seq = -1
        self.waypoint_count = 0

        self.groundspeed = None
        self.airspeed = None
        self.heading = None
        self.throttle = None
        self.altitude = None
        self.climb = None

        self.latitude = None
        self.longitude = None
        self.gps_altitude = None
        self.gps_status = None
        self.gps_service = None

        self.state_received = False
        self.last_status_text = None

        self.set_status("MISSION STATE READER ACTIVE")

    def set_status(self, text):
        if text != self.last_status_text:
            self.get_logger().info(text)
            self.last_status_text = text

    def local_timestamp(self):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def unix_time(self):
        return time.time()

    def ros_stamp_to_float(self, stamp):
        return float(stamp.sec) + float(stamp.nanosec) * 1e-9

    def base_payload(self, ros_time=None):
        return {
            "local_timestamp": self.local_timestamp(),
            "unix_time": self.unix_time(),
            "ros_time": ros_time,
            "source_node": "mission_state_reader_node",
        }

    def publish_json(self, publisher, payload):
        msg = String()
        msg.data = json.dumps(payload, separators=(",", ":"))
        publisher.publish(msg)

    def classify_mode(self, mode):
        m = str(mode).upper()

        if "RTL" in m or "RTH" in m or "RETURN" in m:
            return "RETURN_HOME"

        if "AUTO" in m and "RTL" not in m:
            return "AUTO_MISSION"

        if "GUIDED" in m:
            return "GUIDED"

        if "MANUAL" in m:
            return "MANUAL"

        if "LOITER" in m or "HOLD" in m:
            return "HOLD"

        if "STABILIZE" in m:
            return "STABILIZE"

        return "OTHER"

    def state_callback(self, msg):
        self.state_received = True

        mode_class = self.classify_mode(msg.mode)

        self.last_connected = bool(msg.connected)
        self.last_armed = bool(msg.armed)
        self.last_guided = bool(msg.guided)
        self.last_manual_input = bool(msg.manual_input)
        self.last_mode = str(msg.mode)
        self.last_mode_class = mode_class
        self.last_system_status = int(msg.system_status)

        if msg.armed != self.previous_armed:
            event_type = "ARM" if msg.armed else "DISARM"
            self.publish_event(
                event_type=event_type,
                detail=f"mode={msg.mode}, connected={msg.connected}",
            )

        if msg.mode != self.previous_mode:
            self.publish_event(
                event_type="MODE_CHANGE",
                detail=f"{self.previous_mode} -> {msg.mode}",
                extra={
                    "old_mode": self.previous_mode,
                    "new_mode": msg.mode,
                    "old_mode_class": self.previous_mode_class,
                    "new_mode_class": mode_class,
                },
            )

            if mode_class == "RETURN_HOME":
                self.publish_event(
                    event_type="RETURN_HOME_DETECTED",
                    detail=f"mode={msg.mode}",
                )

        self.previous_armed = bool(msg.armed)
        self.previous_mode = str(msg.mode)
        self.previous_mode_class = mode_class

    def waypoints_callback(self, msg):
        self.current_waypoint_seq = int(msg.current_seq)
        self.waypoint_count = len(msg.waypoints)

        waypoints = []

        for idx, wp in enumerate(msg.waypoints):
            waypoints.append(
                {
                    "index": idx,
                    "is_current": bool(wp.is_current),
                    "autocontinue": bool(wp.autocontinue),
                    "frame": int(wp.frame),
                    "command": int(wp.command),
                    "param1": float(wp.param1),
                    "param2": float(wp.param2),
                    "param3": float(wp.param3),
                    "param4": float(wp.param4),
                    "x_lat": float(wp.x_lat),
                    "y_long": float(wp.y_long),
                    "z_alt": float(wp.z_alt),
                }
            )

        payload = self.base_payload()
        payload.update(
            {
                "type": "WAYPOINT_LIST",
                "current_seq": int(msg.current_seq),
                "waypoint_count": len(msg.waypoints),
                "waypoints": waypoints,
            }
        )

        self.publish_json(self.waypoints_pub, payload)

    def waypoint_reached_callback(self, msg):
        self.last_reached_waypoint_seq = int(msg.wp_seq)
        ros_time = self.ros_stamp_to_float(msg.header.stamp)

        self.publish_event(
            event_type="WAYPOINT_REACHED",
            detail=f"wp_seq={msg.wp_seq}",
            ros_time=ros_time,
            extra={
                "frame_id": msg.header.frame_id,
                "wp_seq": int(msg.wp_seq),
            },
        )

    def vfr_hud_callback(self, msg):
        self.airspeed = float(msg.airspeed)
        self.groundspeed = float(msg.groundspeed)
        self.heading = int(msg.heading)
        self.throttle = float(msg.throttle)
        self.altitude = float(msg.altitude)
        self.climb = float(msg.climb)

    def gps_callback(self, msg):
        self.latitude = float(msg.latitude)
        self.longitude = float(msg.longitude)
        self.gps_altitude = float(msg.altitude)
        self.gps_status = int(msg.status.status)
        self.gps_service = int(msg.status.service)

    def statustext_callback(self, msg):
        ros_time = self.ros_stamp_to_float(msg.header.stamp)

        severity = int(msg.severity)
        text = str(msg.text)

        self.publish_event(
            event_type="STATUSTEXT",
            detail=text,
            ros_time=ros_time,
            extra={
                "frame_id": msg.header.frame_id,
                "severity": severity,
                "text": text,
            },
        )

        upper_text = text.upper()
        if (
            "RTL" in upper_text
            or "RTH" in upper_text
            or "RETURN" in upper_text
            or "FAILSAFE" in upper_text
        ):
            self.publish_event(
                event_type="AUTOPILOT_STATUS_ALERT",
                detail=text,
                ros_time=ros_time,
                extra={
                    "frame_id": msg.header.frame_id,
                    "severity": severity,
                    "text": text,
                },
            )

    def publish_event(self, event_type, detail, ros_time=None, extra=None):
        payload = self.base_payload(ros_time)
        payload.update(
            {
                "type": "EVENT",
                "event_type": event_type,
                "detail": detail,
                "connected": self.last_connected,
                "armed": self.last_armed,
                "guided": self.last_guided,
                "manual_input": self.last_manual_input,
                "mode": self.last_mode,
                "mode_class": self.last_mode_class,
                "system_status": self.last_system_status,
                "current_waypoint_seq": self.current_waypoint_seq,
                "last_reached_waypoint_seq": self.last_reached_waypoint_seq,
                "waypoint_count": self.waypoint_count,
                "groundspeed": self.groundspeed,
                "airspeed": self.airspeed,
                "heading": self.heading,
                "throttle": self.throttle,
                "altitude": self.altitude,
                "climb": self.climb,
                "latitude": self.latitude,
                "longitude": self.longitude,
                "gps_altitude": self.gps_altitude,
                "gps_status": self.gps_status,
                "gps_service": self.gps_service,
            }
        )

        if extra:
            payload.update(extra)

        self.publish_json(self.event_pub, payload)

    def publish_snapshot(self):
        payload = self.base_payload()
        payload.update(
            {
                "type": "SNAPSHOT",
                "state_received": self.state_received,
                "connected": self.last_connected,
                "armed": self.last_armed,
                "guided": self.last_guided,
                "manual_input": self.last_manual_input,
                "mode": self.last_mode,
                "mode_class": self.last_mode_class,
                "system_status": self.last_system_status,
                "current_waypoint_seq": self.current_waypoint_seq,
                "last_reached_waypoint_seq": self.last_reached_waypoint_seq,
                "waypoint_count": self.waypoint_count,
                "groundspeed": self.groundspeed,
                "airspeed": self.airspeed,
                "heading": self.heading,
                "throttle": self.throttle,
                "altitude": self.altitude,
                "climb": self.climb,
                "latitude": self.latitude,
                "longitude": self.longitude,
                "gps_altitude": self.gps_altitude,
                "gps_status": self.gps_status,
                "gps_service": self.gps_service,
            }
        )

        self.publish_json(self.snapshot_pub, payload)


def main(args=None):
    rclpy.init(args=args)

    node = MissionStateReaderNode()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()