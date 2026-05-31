#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SEANO Logger Node v2 — Upgraded Edition
========================================

Perubahan utama dari v1:
  - Hapus total: sync_output_delay, frame_status, frame_span_ms,
    valid_sensor_count, istilah "partial" untuk frame.
  - Sinkronisasi event-driven sebagai mekanisme utama; timer sebagai fallback.
  - sync_time dikembalikan ke slot waktu bulat/detik 00.
  - Status per sensor: valid / stale / missing (tanpa frame-level aggregation). Header timestamp disaring agar tidak menghasilkan delay palsu.
  - XLSX export atomic via /tmp; tidak meninggalkan .xlsx_tmp_* di folder sensor.
  - Auto-mount SSD exFAT via subprocess.run([...]), rate-limited, aman.
  - Internet monitor: ONLINE/OFFLINE saja, tanpa MQTT/cloud.
  - Terminal output minimal.

Node name:     logger_node
Compatibility: ROS2 Humble, Jetson Orin Nano, Ubuntu.
"""

import csv
import glob
import math
import os
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import zipfile
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from html import escape as xml_escape
from typing import Any, Dict, List, Optional

import psutil
import rclpy
from rclpy.executors import SingleThreadedExecutor
from rclpy.node import Node
from rclpy.qos import qos_profile_sensor_data

from sensor_msgs.msg import BatteryState, Imu, NavSatFix
from std_msgs.msg import Float64MultiArray
from mavros_msgs.msg import (
    State,
    StatusText,
    VfrHud,
    WaypointList,
    WaypointReached,
)


# =============================================================================
# CONSTANTS — tuning defaults; semua bisa dioverride via ROS parameter
# =============================================================================

# Sinkronisasi
SYNC_RATE_HZ            = 1.0       # Hz
SYNC_TOLERANCE_MS       = 500.0     # ms; selisih maks timestamp sensor vs sync_time
HEADER_TIME_MAX_SKEW_MS = 2000.0    # ms; header ROS dipercaya hanya jika dekat dengan receive wall time
EVENT_SYNC_WINDOW_MS    = 350.0     # ms; tunggu setelah semua sensor fresh sebelum tulis
SENSOR_FRESH_TIMEOUT_MS = 2000.0    # ms; sensor dianggap fresh jika sampelnya dalam waktu ini
WRITE_ROWS_WITH_MISSING = True      # tulis baris walau ada sensor missing

ENABLE_EVENT_SYNC       = True      # gunakan event-driven sync sebagai primer
ENABLE_TIMER_FALLBACK   = True      # gunakan timer sebagai fallback

# Buffer
SENSOR_BUFFER_MAXLEN = 500          # jumlah sample per sensor dalam rolling buffer
STATS_MAXLEN         = 7200         # batas statistik delta/age (~2 jam @1 Hz)

# Interval timer
FLUSH_INTERVAL_SEC    = 0.5
METRICS_INTERVAL_SEC  = 1.0
TIMELINE_INTERVAL_SEC = 1.0
INTERNET_CHECK_SEC    = 5.0
STORAGE_CHECK_SEC     = 2.0

# I/O buffering
CSV_BUFFER_BYTES = 8192             # sensor individual CSV (high-freq, batched)
SYNC_CSV_BUFFER  = 1                # synchronized_log & diagnostics: line-buffered (real-time)
EVENT_BUFFER     = 1                # event log: line-buffered

# XLSX — default True agar synchronized_log.xlsx dibuat otomatis setelah disarm
ENABLE_XLSX_EXPORT = True

# Write queue cap
WRITE_QUEUE_MAXLEN = 2000

# Internet probe
INTERNET_HOST        = "1.1.1.1"
INTERNET_PORT        = 53
INTERNET_TIMEOUT_SEC = 0.5

# SSD auto-mount
SSD_UUID                 = "4028-495B"
SSD_DEVICE               = f"/dev/disk/by-uuid/{SSD_UUID}"
SSD_DEFAULT_MOUNTPOINT   = "/mnt/seano/SEANO_SSD"
SSD_MOUNT_RATE_LIMIT_SEC = 10.0     # maks 1 percobaan mount tiap 10 detik

SENSORS = ["gps", "imu", "ctd", "adcp", "battery"]


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class SensorSample:
    """Satu sample sensor dengan timestamp referensi."""
    sensor:              str
    recv_wall_ns:        int            # time.time_ns() saat diterima
    msg_ros_ns:          Optional[int]  # dari header ROS; None jika tidak ada
    mission_elapsed_sec: float
    payload:             Dict[str, Any]


@dataclass
class SyncStats:
    """Statistik sinkronisasi per sensor, dibatasi maxlen."""
    delta_ms: deque = field(default_factory=lambda: deque(maxlen=STATS_MAXLEN))
    age_ms:   deque = field(default_factory=lambda: deque(maxlen=STATS_MAXLEN))
    valid:    int   = 0
    stale:    int   = 0
    missing:  int   = 0


# =============================================================================
# MATH HELPERS
# =============================================================================

def _mean(values) -> float:
    return sum(values) / len(values) if values else math.nan


def _median(values) -> float:
    if not values:
        return math.nan
    s = sorted(values)
    n = len(s)
    mid = n // 2
    return float(s[mid]) if n % 2 else 0.5 * (s[mid - 1] + s[mid])


def _median_int(values) -> float:
    """Median dari list integer/float; return float."""
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    mid = n // 2
    return float(s[mid]) if n % 2 else (s[mid - 1] + s[mid]) / 2.0


def haversine_m(lat1, lon1, lat2, lon2) -> float:
    R = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return R * 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


# =============================================================================
# SSD MOUNT HELPERS — module-level, aman dipanggil kapan saja
# =============================================================================

def is_mountpoint(path: str) -> bool:
    """True jika path adalah mounted filesystem."""
    try:
        return os.path.ismount(path)
    except Exception:
        return False


def is_path_writable(path: str) -> bool:
    """True jika path ada dan bisa ditulis (write-probe)."""
    if not os.path.exists(path):
        return False
    probe = os.path.join(path, ".seano_write_probe")
    try:
        with open(probe, "w") as f:
            f.write("ok")
        os.remove(probe)
        return True
    except Exception:
        return False


def try_auto_mount_external(
    device: str = SSD_DEVICE,
    mountpoint: str = SSD_DEFAULT_MOUNTPOINT,
    logger_warn=None,
    logger_err=None,
) -> bool:
    """
    Coba mount SSD exFAT jika device ada tapi belum di-mount.

    Aturan keamanan:
      - Tidak unmount apa pun
      - Tidak format apa pun
      - Tidak menghapus atau memindahkan konten mountpoint
      - Pakai subprocess.run([...]), bukan shell string

    Auto-mount tanpa password membutuhkan entri sudoers:
      <user> ALL=(ALL) NOPASSWD: /usr/sbin/mount.exfat-fuse
    atau gunakan systemd automount / udev rules.

    Returns True jika sudah/berhasil mount, False jika gagal.
    """
    # 1. Device ada?
    if not os.path.exists(device):
        return False  # SSD tidak terdeteksi

    # 2. Sudah mount?
    if is_mountpoint(mountpoint):
        return True

    # 3. Buat mountpoint jika belum ada (hanya mkdir, tidak hapus isi)
    try:
        os.makedirs(mountpoint, exist_ok=True)
    except Exception as exc:
        if logger_err:
            logger_err(f"Gagal buat mountpoint {mountpoint}: {exc}")
        return False

    # 4. Cari mount.exfat-fuse
    mounter = (
        shutil.which("mount.exfat-fuse")
        or shutil.which("mount.exfat")
        or "/usr/sbin/mount.exfat-fuse"
    )
    if not os.path.exists(mounter):
        if logger_warn:
            logger_warn(
                "mount.exfat-fuse tidak ditemukan. "
                "Jalankan: sudo apt install exfat-fuse exfatprogs"
            )
        return False

    uid = os.getuid()
    gid = os.getgid()
    options = f"uid={uid},gid={gid},umask=002"
    base_cmd = [mounter, "-o", options, device, mountpoint]

    # 5. Coba langsung hanya jika process berjalan sebagai root.
    # Jika non-root langsung menjalankan mount.exfat-fuse, journal akan berisi
    # "failed to open device: Permission denied". Itu noise dan tidak berguna.
    if os.geteuid() == 0:
        try:
            r = subprocess.run(base_cmd, capture_output=True, text=True, timeout=10)
            if r.returncode == 0 and is_mountpoint(mountpoint):
                return True
            if logger_warn and r.returncode != 0:
                logger_warn(f"SSD direct mount gagal (code={r.returncode}): {r.stderr.strip()}")
        except Exception as exc:
            if logger_warn:
                logger_warn(f"SSD direct mount exception: {exc}")

    # 6. Coba dengan sudo --non-interactive (butuh sudoers NOPASSWD)
    sudo_cmd = ["sudo", "--non-interactive"] + base_cmd
    try:
        r2 = subprocess.run(sudo_cmd, capture_output=True, text=True, timeout=10)
        if r2.returncode == 0 and is_mountpoint(mountpoint):
            return True
        if logger_warn:
            detail = r2.stderr.strip()
            logger_warn(
                f"SSD mount gagal (code={r2.returncode}). "
                f"Butuh sudoers NOPASSWD untuk command mount.exfat-fuse SSD SEANO. "
                f"Logger fallback ke local jika local enabled. Detail: {detail}"
            )
    except Exception as exc:
        if logger_warn:
            logger_warn(f"SSD mount exception: {exc}")

    return False


# =============================================================================
# XLSX WRITER — zero extra dependency, inline string only
# =============================================================================

def _xlsx_col_name(index: int) -> str:
    name = ""
    while index > 0:
        index, r = divmod(index - 1, 26)
        name = chr(65 + r) + name
    return name


def _clean_xml(value) -> str:
    if value is None:
        return ""
    return "".join(
        ch for ch in str(value)
        if ch in ("\t", "\n", "\r") or ord(ch) >= 32
    )


def _safe_sheet_name(name: str, used: set) -> str:
    invalid = set('[]:*?/\\')
    cleaned = "".join("_" if c in invalid else c for c in str(name)).strip() or "Sheet"
    cleaned = cleaned[:31]
    base, counter = cleaned, 1
    while cleaned in used:
        suffix = f"_{counter}"
        cleaned = base[:31 - len(suffix)] + suffix
        counter += 1
    used.add(cleaned)
    return cleaned


def write_xlsx(xlsx_path: str, sources: List[tuple]):
    """
    Tulis file XLSX dari list (sheet_name, csv_path).
    Semua cell ditulis sebagai inline string.
    """
    used_names: set = set()
    sheets = [(idx, _safe_sheet_name(n, used_names), p)
               for idx, (n, p) in enumerate(sources, 1)]

    with zipfile.ZipFile(xlsx_path, "w", zipfile.ZIP_DEFLATED) as zf:
        overrides = "".join(
            f'<Override PartName="/xl/worksheets/sheet{i}.xml" '
            f'ContentType="application/vnd.openxmlformats-officedocument'
            f'.spreadsheetml.worksheet+xml"/>'
            for i, _, _ in sheets
        )
        zf.writestr(
            "[Content_Types].xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-'
            'officedocument.spreadsheetml.sheet.main+xml"/>'
            + overrides + '</Types>',
        )
        zf.writestr(
            "_rels/.rels",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/'
            'relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>',
        )
        sheet_elems = "".join(
            f'<sheet name="{xml_escape(sn, quote=True)}" sheetId="{i}" r:id="rId{i}"/>'
            for i, sn, _ in sheets
        )
        zf.writestr(
            "xl/workbook.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            f'<sheets>{sheet_elems}</sheets></workbook>',
        )
        rels = "".join(
            f'<Relationship Id="rId{i}" Type="http://schemas.openxmlformats.org/officeDocument'
            f'/2006/relationships/worksheet" Target="worksheets/sheet{i}.xml"/>'
            for i, _, _ in sheets
        )
        zf.writestr(
            "xl/_rels/workbook.xml.rels",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            + rels + '</Relationships>',
        )
        for i, sn, csv_path in sheets:
            with zf.open(f"xl/worksheets/sheet{i}.xml", "w") as sf:
                sf.write(
                    b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                    b'<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                    b'<sheetData>'
                )
                try:
                    with open(csv_path, "r", newline="", encoding="utf-8") as f:
                        for ridx, row in enumerate(csv.reader(f), 1):
                            sf.write(f'<row r="{ridx}">'.encode())
                            for cidx, val in enumerate(row, 1):
                                cref = f"{_xlsx_col_name(cidx)}{ridx}"
                                text = _clean_xml(val)
                                if not text:
                                    sf.write(f'<c r="{cref}"/>'.encode())
                                    continue
                                preserve = ' xml:space="preserve"' if text.strip() != text else ""
                                sf.write(
                                    f'<c r="{cref}" t="inlineStr"><is><t{preserve}>'
                                    f'{xml_escape(text, quote=False)}</t></is></c>'.encode()
                                )
                            sf.write(b'</row>')
                except Exception as exc:
                    sf.write(
                        f'<row r="1"><c r="A1" t="inlineStr"><is><t>'
                        f'{xml_escape(str(exc), quote=False)}</t></is></c></row>'.encode()
                    )
                sf.write(b'</sheetData></worksheet>')


# =============================================================================
# MAIN NODE
# =============================================================================

class SeanoLogger(Node):

    # -------------------------------------------------------------------------
    # INIT
    # -------------------------------------------------------------------------
    def __init__(self):
        super().__init__("logger_node")

        # --- ROS parameters ---------------------------------------------------
        self.declare_parameter("external_mount_point",       SSD_DEFAULT_MOUNTPOINT)
        self.declare_parameter("local_mount_point",
                               os.path.expanduser("~/Documents/SEANO_logs"))
        self.declare_parameter("enable_external_logging",    True)
        self.declare_parameter("enable_local_logging",       True)
        self.declare_parameter("require_external_on_mission", False)
        self.declare_parameter("enable_xlsx_export",         ENABLE_XLSX_EXPORT)

        self.declare_parameter("sync_rate_hz",               SYNC_RATE_HZ)
        self.declare_parameter("sync_tolerance_ms",          SYNC_TOLERANCE_MS)
        self.declare_parameter("header_time_max_skew_ms",    HEADER_TIME_MAX_SKEW_MS)
        self.declare_parameter("event_sync_window_ms",       EVENT_SYNC_WINDOW_MS)
        self.declare_parameter("sensor_fresh_timeout_ms",    SENSOR_FRESH_TIMEOUT_MS)
        self.declare_parameter("write_rows_with_missing_sensor", WRITE_ROWS_WITH_MISSING)
        self.declare_parameter("enable_event_sync",          ENABLE_EVENT_SYNC)
        self.declare_parameter("enable_timer_fallback_sync", ENABLE_TIMER_FALLBACK)
        self.declare_parameter("sensor_buffer_maxlen",       SENSOR_BUFFER_MAXLEN)

        # --- Read params (node-lifetime values) --------------------------------
        self.external_mount   = self.get_parameter("external_mount_point").value
        self.local_mount      = self.get_parameter("local_mount_point").value
        self.enable_ext       = self.get_parameter("enable_external_logging").value
        self.enable_local     = self.get_parameter("enable_local_logging").value
        self.require_ext      = self.get_parameter("require_external_on_mission").value
        self.enable_xlsx      = self.get_parameter("enable_xlsx_export").value

        self.sync_rate_hz            = float(self.get_parameter("sync_rate_hz").value)
        self.sync_period_ns          = int(1e9 / self.sync_rate_hz)
        self.sync_tolerance_ms       = float(self.get_parameter("sync_tolerance_ms").value)
        self.header_time_max_skew_ms = float(self.get_parameter("header_time_max_skew_ms").value)
        self.event_sync_window_ms    = float(self.get_parameter("event_sync_window_ms").value)
        self.ev_sync_window_ns       = int(self.event_sync_window_ms * 1e6)
        self.sensor_fresh_timeout_ms = float(self.get_parameter("sensor_fresh_timeout_ms").value)
        self.write_rows_with_missing = bool(
            self.get_parameter("write_rows_with_missing_sensor").value)
        self.enable_event_sync       = bool(self.get_parameter("enable_event_sync").value)
        self.enable_timer_fallback   = bool(
            self.get_parameter("enable_timer_fallback_sync").value)
        self.sensor_buffer_maxlen    = int(self.get_parameter("sensor_buffer_maxlen").value)

        # --- Persistent latest sensor cache ----------------------------------
        # Dipertahankan lintas reset sesi supaya nilai battery sebelum ARM
        # tidak hilang saat _start_session() memanggil _reset_runtime_state().
        self.latest_batt_voltage = math.nan
        self.latest_batt_current = math.nan
        self.latest_batt_percent = math.nan

        # --- Terminal status de-duplication ----------------------------------
        self._last_terminal_status = None
        self._last_storage_status = None

        # --- Runtime state (per session) --------------------------------------
        self._reset_runtime_state()

        # --- Internet monitor — daemon thread (TCP probe memblokir, harus terpisah) ---
        self._internet_lock           = threading.Lock()
        self._internet_online         = None   # None = belum diketahui
        self._internet_lost_count     = 0
        self._internet_total_down_sec = 0.0
        self._internet_lost_since     = None
        self._internet_thread = threading.Thread(
            target=self._internet_worker, daemon=True, name="inet_monitor"
        )
        self._internet_thread.start()

        # --- Subscribers ------------------------------------------------------
        self.create_subscription(State, "/mavros/state",
                                 self._mavros_state_cb, 10)
        self.create_subscription(WaypointList, "/mavros/mission/waypoints",
                                 self._waypoints_cb, qos_profile_sensor_data)
        self.create_subscription(WaypointReached, "/mavros/mission/reached",
                                 self._wp_reached_cb, qos_profile_sensor_data)
        self.create_subscription(VfrHud, "/mavros/vfr_hud",
                                 self._vfr_hud_cb, qos_profile_sensor_data)
        self.create_subscription(StatusText, "/mavros/statustext/recv",
                                 self._statustext_cb, qos_profile_sensor_data)
        self.create_subscription(NavSatFix, "/mavros/global_position/raw/fix",
                                 self._gps_cb, qos_profile_sensor_data)
        self.create_subscription(Imu, "/mavros/imu/data",
                                 self._imu_cb, qos_profile_sensor_data)
        self.create_subscription(Float64MultiArray, "/ctd/data",
                                 self._ctd_cb, 50)
        self.create_subscription(Float64MultiArray, "/adcp/data",
                                 self._adcp_cb, 10)
        self.create_subscription(BatteryState, "/battery/state",
                                 self._battery_cb, qos_profile_sensor_data)

        # --- Timers -----------------------------------------------------------
        self.create_timer(1.0 / self.sync_rate_hz, self._sync_timer_cb)
        self.create_timer(FLUSH_INTERVAL_SEC,       self._flush_write_queues)
        self.create_timer(METRICS_INTERVAL_SEC,     self._log_metrics)
        self.create_timer(TIMELINE_INTERVAL_SEC,    self._log_timeline)
        self.create_timer(STORAGE_CHECK_SEC,        self._monitor_storage)

        psutil.cpu_percent(interval=None)  # prime cpu counter
        self._status("LOGGER_NODE: READY | STANDBY_WAITING_ARM")

    # -------------------------------------------------------------------------
    # RUNTIME STATE RESET
    # -------------------------------------------------------------------------
    def _reset_runtime_state(self):
        """Reset semua state per sesi logging."""
        self.logging_active      = False
        self.start_time_obj      = None
        self.end_time_obj        = None
        self.mission_start_mono  = None
        self.mission_id          = None
        self.base_paths: List[str] = []
        self.local_tz            = time.tzname[0]

        # MAVROS state
        self.mav_connected   = False
        self.mav_armed       = False
        self.mav_guided      = False
        self.mav_manual      = False
        self.mav_mode        = "UNKNOWN"
        self.mav_mode_class  = "OTHER"
        self.mav_sys_status  = 0
        self.prev_mode       = "UNKNOWN"
        self.prev_mode_class = "OTHER"

        # Waypoints
        self.wp_current      = -1
        self.wp_last_reached = -1
        self.wp_count        = 0
        self.wp_last_msg     = None
        self.wp_signature    = None

        # VFR HUD
        self.vfr_groundspeed = math.nan
        self.vfr_airspeed    = math.nan
        self.vfr_heading     = math.nan
        self.vfr_throttle    = math.nan
        self.vfr_altitude    = math.nan
        self.vfr_climb       = math.nan

        # GPS
        self.gps_lat     = math.nan
        self.gps_lon     = math.nan
        self.gps_alt     = math.nan
        self.gps_status  = math.nan
        self.gps_service = math.nan

        # Battery
        # Ambil dari cache terakhir sebelum ARM jika sudah ada. Ini mencegah
        # Start battery kosong akibat _reset_runtime_state() menghapus nilai
        # battery tepat saat mission mulai.
        self.batt_voltage  = getattr(self, 'latest_batt_voltage', math.nan)
        self.batt_current  = getattr(self, 'latest_batt_current', math.nan)
        self.batt_percent  = getattr(self, 'latest_batt_percent', math.nan)
        self.batt_start    = math.nan
        self.batt_end      = math.nan

        # Mission stats
        self.event_count        = 0
        self.rtl_detected       = False
        self.failsafe_detected  = False
        self.max_speed          = 0.0
        self.speed_sum          = 0.0
        self.speed_samples      = 0
        self.distance_m         = 0.0
        self.dist_last_lat      = None
        self.dist_last_lon      = None
        self.last_timeline_slot = -1
        self.last_timeline_time = 0.0
        self.last_internet_slot = -1

        # Sensor buffers — diakses hanya dari ROS callback (SingleThreadedExecutor)
        buf_len = getattr(self, "sensor_buffer_maxlen", SENSOR_BUFFER_MAXLEN)
        self.buffers: Dict[str, deque] = {
            s: deque(maxlen=buf_len) for s in SENSORS
        }
        self.last_rx_ns: Dict[str, int]    = {s: 0 for s in SENSORS}
        self.sample_count: Dict[str, int]  = {s: 0 for s in SENSORS}
        self.sensor_csv_slot: Dict[str, int] = {s: -1 for s in SENSORS}

        # Write queue: diakses callback + timer flush → lock tipis
        self._wq_lock = threading.Lock()
        self.write_queues: Dict[str, deque] = {
            s: deque(maxlen=WRITE_QUEUE_MAXLEN) for s in SENSORS
        }

        # Sync state — tanpa frame_status, tanpa frame-level counters
        self.last_sync_write_wall_ns = 0  # wall-time saat row terakhir ditulis
        self.last_sync_target_ns = 0       # target sync_time terakhir, aligned ke detik 00
        self.sync_total = 0
        self.sync_stats: Dict[str, SyncStats] = {s: SyncStats() for s in SENSORS}

        # Event-driven sync state
        self.ev_sync_pending        = False
        self.ev_trigger_ns          = 0
        self.ev_last_sensor_ns: Dict[str, int] = {s: 0 for s in SENSORS}

        # Storage
        self.ext_ready         = False
        self.ext_fail_logged   = False
        self._last_mount_attempt = 0.0  # rate-limit mount attempt

        # Misc
        self.adcp_warn_reported = False
        self.bytes_written      = 0
        self.last_metrics_time  = time.time()

        self._reset_file_handles()

    def _reset_file_handles(self):
        self.sensor_files:   Dict[str, list] = {s: [] for s in SENSORS}
        self.sensor_writers: Dict[str, list] = {s: [] for s in SENSORS}

        self.sync_files:          list = []
        self.sync_writers:        list = []
        self.diag_files:          list = []
        self.diag_writers:        list = []
        self.metrics_files:       list = []
        self.metrics_writers:     list = []
        self.timeline_files:      list = []
        self.timeline_writers:    list = []
        self.readable_files:      list = []
        self.readable_writers:    list = []
        self.events_files:        list = []
        self.events_writers:      list = []
        self.state_files:         list = []
        self.state_writers:       list = []
        self.wp_reached_files:    list = []
        self.wp_reached_writers:  list = []
        self.waypoints_files:     list = []
        self.waypoints_writers:   list = []
        self.statustext_files:    list = []
        self.statustext_writers:  list = []
        self.inet_status_files:   list = []
        self.inet_status_writers: list = []
        self.inet_event_files:    list = []
        self.mission_log_files:   list = []

        self.mission_info_paths:   list = []
        self.summary_paths:        list = []
        self.sync_quality_paths:   list = []

    # -------------------------------------------------------------------------
    # LOGGING HELPERS (minimal terminal output)
    # -------------------------------------------------------------------------
    def _status(self, text: str):
        """Print terminal status hanya saat berubah supaya tidak spam."""
        if text == self._last_terminal_status:
            return
        self._last_terminal_status = text
        print(text, flush=True)

    def _storage_status(self, text: str):
        """Print status SSD hanya saat berubah supaya terminal tetap bersih."""
        if text == self._last_storage_status:
            return
        self._last_storage_status = text
        print(text, flush=True)

    def _warn(self, text: str):
        self.get_logger().warning(text)

    def _err(self, text: str):
        self.get_logger().error(text)

    # -------------------------------------------------------------------------
    # TIME HELPERS
    # -------------------------------------------------------------------------
    def _wall_ns(self) -> int:
        return time.time_ns()

    def _elapsed(self) -> float:
        if self.mission_start_mono is None:
            return 0.0
        return time.monotonic() - self.mission_start_mono

    def _ts_now(self) -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def _ts_ns(self, ns: int) -> str:
        return datetime.fromtimestamp(ns / 1e9).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def _ts_current_second(self) -> str:
        """Timestamp wall-clock saat ini, dibulatkan turun ke detik .000."""
        now_ns = self._wall_ns()
        sec_ns = (now_ns // 1_000_000_000) * 1_000_000_000
        return self._ts_ns(sec_ns)

    def _unix_now(self) -> float:
        return time.time()

    def _header_ns(self, msg) -> Optional[int]:
        """Ekstrak timestamp dari header ROS. Return None jika tidak valid."""
        try:
            s = msg.header.stamp
            ns = int(s.sec) * 1_000_000_000 + int(s.nanosec)
            return ns if ns > 0 else None
        except Exception:
            return None

    def _safe(self, v):
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return ""
        return v

    def _classify_mode(self, mode: str) -> str:
        m = mode.upper()
        if "RTL" in m or "RTH" in m or "RETURN" in m:
            return "RETURN_HOME"
        if "AUTO" in m:
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

    # -------------------------------------------------------------------------
    # STORAGE HELPERS
    # -------------------------------------------------------------------------
    def _open_csv(self, path: str, header: list,
                  file_list: list, writer_list: list,
                  buffering: int = CSV_BUFFER_BYTES):
        f = open(path, "w", newline="", buffering=buffering)
        w = csv.writer(f)
        w.writerow(header)
        file_list.append(f)
        writer_list.append(w)

    # -------------------------------------------------------------------------
    # AUTO-MOUNT: EXTERNAL STORAGE PREPARATION
    # -------------------------------------------------------------------------
    def _prepare_external_storage(self):
        """
        Cek status SSD dan coba auto-mount jika perlu.
        Dipanggil sebelum _prepare_base_paths() saat ARM.
        Rate-limited: maksimal sekali tiap SSD_MOUNT_RATE_LIMIT_SEC.
        """
        mount = self.external_mount

        # Sudah mount dan bisa ditulis?
        if is_mountpoint(mount) and is_path_writable(mount):
            if not self.ext_ready:
                self.ext_ready = True
                self._storage_status("SSD MOUNTED")
            return

        # Rate-limit percobaan mount
        now = time.time()
        if (now - self._last_mount_attempt) < SSD_MOUNT_RATE_LIMIT_SEC:
            self.ext_ready = is_mountpoint(mount) and is_path_writable(mount)
            return
        self._last_mount_attempt = now

        # Coba auto-mount
        success = try_auto_mount_external(
            device=SSD_DEVICE,
            mountpoint=mount,
            logger_warn=self._warn,
            logger_err=self._err,
        )

        if success and is_path_writable(mount):
            self.ext_ready = True
            self._storage_status("SSD MOUNTED")
        else:
            self.ext_ready = False
            self._storage_status("SSD NOT MOUNTED | fallback local")

    def _prepare_base_paths(self, year: str, month: str, day: str) -> bool:
        """Siapkan direktori logging. External storage diproses lebih dulu."""
        self.base_paths = []

        if self.enable_ext:
            self._prepare_external_storage()

            if self.ext_ready:
                ext_path = os.path.join(
                    self.external_mount, "SEANO_MISSIONS", year, month, day,
                    self.mission_id
                )
                try:
                    os.makedirs(ext_path, exist_ok=True)
                    if not is_path_writable(ext_path):
                        raise RuntimeError("not writable after makedirs")
                    self.base_paths.append(ext_path)
                except Exception as exc:
                    self._err(f"External path error: {ext_path} | {exc}")
                    self.ext_ready = False

            if self.require_ext and not self.ext_ready:
                self._err(
                    "require_external_on_mission=True tapi SSD tidak tersedia. "
                    "Logger standby."
                )
                return False

        if self.enable_local:
            local_path = os.path.join(
                self.local_mount, year, month, day, self.mission_id
            )
            try:
                os.makedirs(local_path, exist_ok=True)
                if not is_path_writable(local_path):
                    raise RuntimeError("not writable")
                self.base_paths.append(local_path)
            except Exception as exc:
                self._err(f"Local storage failed: {local_path} | {exc}")

        return len(self.base_paths) > 0

    def _monitor_storage(self):
        if not self.logging_active or not self.enable_ext or not self.ext_ready:
            return
        if not is_mountpoint(self.external_mount) or not is_path_writable(self.external_mount):
            if not self.ext_fail_logged:
                self._storage_status("SSD LOST")
                self._err(f"STORAGE FATAL: SSD hilang saat misi: {self.external_mount}")
                self.ext_fail_logged = True
                self.ext_ready = False

    # -------------------------------------------------------------------------
    # SESSION: START
    # -------------------------------------------------------------------------
    def _start_session(self, state_msg=None):
        if self.logging_active:
            return

        self._status("LOGGER_NODE: STARTING")
        self._reset_runtime_state()

        self.start_time_obj     = datetime.now()
        self.mission_start_mono = time.monotonic()
        self.local_tz           = time.tzname[0]
        self.mission_id         = self.start_time_obj.strftime(
            f"MISSION_START_%H-%M-%S_{self.local_tz}"
        )

        y = self.start_time_obj.strftime("%Y")
        m = self.start_time_obj.strftime("%m")
        d = self.start_time_obj.strftime("%d")

        if not self._prepare_base_paths(y, m, d):
            self._status("LOGGER_NODE: READY | STANDBY_WAITING_ARM")
            return

        self._init_folder_structure()
        self._write_mission_start_info(state_msg)
        self._init_metrics_logger()
        self._init_sensor_loggers()
        self._init_mission_logger()

        self.logging_active    = True
        self.last_metrics_time = time.time()
        self.bytes_written     = 0

        # Start battery diambil dari cache battery terakhir sebelum ARM jika tersedia.
        # Kalau belum tersedia, akan diisi oleh _battery_cb saat sample battery pertama masuk.
        self.batt_start = self.latest_batt_percent
        if not isinstance(self.batt_start, float):
            try:
                self.batt_start = float(self.batt_start)
            except Exception:
                self.batt_start = math.nan

        psutil.cpu_percent(interval=None)

        if state_msg is not None:
            self.prev_mode       = state_msg.mode
            self.prev_mode_class = self._classify_mode(state_msg.mode)

        self._write_event("ARM",
                          f"mode={self.mav_mode}, connected={self.mav_connected}")

        if self.wp_last_msg is not None:
            self._dump_waypoints(self.wp_last_msg)

        self._status(f"LOGGER_NODE: ACTIVE | mission_id={self.mission_id}")

    # -------------------------------------------------------------------------
    # SESSION: STOP
    # -------------------------------------------------------------------------
    def _stop_session(self, reason: str = "vehicle disarmed"):
        if not self.logging_active:
            return

        self.logging_active = False
        self.end_time_obj   = datetime.now()
        self._status("LOGGER_NODE: STOPPING")

        self._flush_write_queues(force=True)
        self._write_sync_quality_summary()
        self._write_mission_end_info(reason)
        self._close_all_files()

        try:
            os.sync()
        except Exception:
            pass

        # XLSX export — synchronous, setelah semua CSV tertutup dan data aman
        if self.enable_xlsx:
            self._export_xlsx_all()

        self._finalize_standby()

    def _finalize_standby(self):
        self._status("LOGGER_NODE: STOPPED")
        self._reset_runtime_state()
        self._status("LOGGER_NODE: READY | STANDBY_WAITING_ARM")

    # -------------------------------------------------------------------------
    # FOLDER STRUCTURE
    # -------------------------------------------------------------------------
    def _init_folder_structure(self):
        for bp in self.base_paths:
            os.makedirs(os.path.join(bp, "mission"), exist_ok=True)
            os.makedirs(os.path.join(bp, "sensor"),  exist_ok=True)
            # folder video/ TIDAK dibuat di sini; dikelola oleh video_logger_node

    # -------------------------------------------------------------------------
    # MISSION INFO FILES
    # -------------------------------------------------------------------------
    def _write_mission_start_info(self, state_msg=None):
        for bp in self.base_paths:
            path = os.path.join(bp, "mission_info.txt")
            self.mission_info_paths.append(path)
            with open(path, "w") as f:
                f.write("SEANO Mission Info\n==================\n")
                f.write(f"Start Time              : {self.start_time_obj}\n")
                f.write(f"Timezone                : {self.local_tz}\n")
                f.write("Platform                : SEANO USV\n")
                f.write("Logger Mode             : Armed gated\n")
                f.write("Logger Type             : Event-driven multi-sensor logger\n")
                f.write(f"Sync Rate Hz            : {self.sync_rate_hz:.3f}\n")
                f.write("Folder Structure        : root + mission/ + sensor/\n")
                f.write("Mission Gate            : /mavros/state armed=True\n")
                f.write("Note                    : folder video/ dikelola oleh video_logger_node\n")
                if state_msg is not None:
                    f.write(f"Start Mode              : {state_msg.mode}\n")
                    f.write(f"Start Connected         : {state_msg.connected}\n")

    def _write_mission_end_info(self, reason: str):
        end     = self.end_time_obj or datetime.now()
        elapsed = self._elapsed()
        self._write_mission_summary(reason, end, elapsed)

        for path in self.mission_info_paths:
            try:
                with open(path, "a") as f:
                    f.write(f"End Time                : {end}\n")
                    f.write(f"Duration sec            : {elapsed:.3f}\n")
                    f.write(f"Stop Reason             : {reason}\n")
                    f.write(f"Total sync rows         : {self.sync_total}\n")
                    f.write(
                        f"Internet final          : "
                        f"{self._inet_text(self._get_internet_status())}\n"
                    )
            except Exception as exc:
                self._err(f"mission_info append failed: {exc}")

    # -------------------------------------------------------------------------
    # INIT: SENSOR LOGGERS
    # -------------------------------------------------------------------------
    def _init_sensor_loggers(self):
        for bp in self.base_paths:
            sd = os.path.join(bp, "sensor")
            os.makedirs(sd, exist_ok=True)

            # synchronized_log dan sync_diagnostics: line-buffered untuk backup realtime
            self._open_csv(
                os.path.join(sd, "synchronized_log.csv"),
                self._sync_data_header(),
                self.sync_files, self.sync_writers,
                buffering=SYNC_CSV_BUFFER,
            )
            self._open_csv(
                os.path.join(sd, "sync_diagnostics.csv"),
                self._sync_diag_header(),
                self.diag_files, self.diag_writers,
                buffering=SYNC_CSV_BUFFER,
            )
            self.sync_quality_paths.append(os.path.join(sd, "sync_quality_summary.csv"))

            for s in SENSORS:
                self._open_csv(
                    os.path.join(sd, f"{s}.csv"),
                    self._sensor_header(s),
                    self.sensor_files[s], self.sensor_writers[s],
                )

    def _init_metrics_logger(self):
        for bp in self.base_paths:
            self._open_csv(
                os.path.join(bp, "system_metrics.csv"),
                ["timestamp", "write_speed_Bps", "cpu_percent", "ram_percent",
                 "jetson_temp_c", "gps_hz", "imu_hz", "ctd_hz", "adcp_hz",
                 "battery_hz", "timeline_hz"],
                self.metrics_files, self.metrics_writers,
            )

    def _init_mission_logger(self):
        for bp in self.base_paths:
            md = os.path.join(bp, "mission")
            os.makedirs(md, exist_ok=True)

            # README
            with open(os.path.join(md, "00_READ_ME_FIRST.txt"), "w") as f:
                f.write("SEANO Mission Log — Quick Guide\n================================\n\n")
                f.write("Root:\n")
                f.write("  mission_info.txt | mission_summary.txt | system_metrics.csv\n\n")
                f.write("mission/ :\n")
                for fn in [
                    "mission_readable.csv", "mission_timeline.csv",
                    "mission_events.csv", "mission_events_readable.log",
                    "mission_state_changes.csv", "mission_waypoint_reached.csv",
                    "mission_waypoints.csv", "mission_statustext.csv",
                    "internet_status.csv", "internet_events.log",
                ]:
                    f.write(f"  {fn}\n")
                f.write("\nsensor/ :\n")
                f.write("  synchronized_log.csv      — semua sensor tersinkronkan\n")
                f.write("  sync_diagnostics.csv      — delay/status per sensor per baris\n")
                f.write("  sync_quality_summary.csv  — statistik kualitas sinkronisasi\n")
                f.write("  gps.csv | imu.csv | ctd.csv | adcp.csv | battery.csv\n")
                f.write("  synchronized_log.xlsx     — dibuat setelah disarm jika enable_xlsx_export=True\n")
                f.write("\nVideo: dikelola oleh video_logger_node (bukan logger_node ini)\n")

            # Event readable log (line-buffered)
            mlog = open(os.path.join(md, "mission_events_readable.log"),
                        "w", buffering=EVENT_BUFFER)
            mlog.write("SEANO Mission Events\n====================\n")
            self.mission_log_files.append(mlog)

            # Internet status
            self._open_csv(
                os.path.join(md, "internet_status.csv"),
                ["time", "elapsed_sec", "internet", "lost_count",
                 "total_down_sec", "availability_percent", "note"],
                self.inet_status_files, self.inet_status_writers,
            )
            inet_ev = open(os.path.join(md, "internet_events.log"),
                           "w", buffering=EVENT_BUFFER)
            inet_ev.write("SEANO Internet Events\n=====================\n")
            self.inet_event_files.append(inet_ev)

            self._open_csv(
                os.path.join(md, "mission_readable.csv"),
                ["time", "elapsed_sec", "status", "mode", "mode_class",
                 "current_wp", "last_reached_wp", "wp_count",
                 "speed_mps", "heading_deg", "throttle",
                 "lat", "lon", "gps_alt", "battery_percent",
                 "internet", "note"],
                self.readable_files, self.readable_writers,
            )
            self._open_csv(
                os.path.join(md, "mission_timeline.csv"),
                ["local_timestamp", "unix_time", "mission_elapsed_sec",
                 "connected", "armed", "guided", "manual_input",
                 "mode", "mode_class", "system_status",
                 "current_waypoint_seq", "last_reached_waypoint_seq", "waypoint_count",
                 "groundspeed_mps", "airspeed_mps", "heading_deg", "throttle",
                 "vfr_altitude", "climb",
                 "latitude", "longitude", "gps_altitude", "gps_status", "gps_service",
                 "battery_voltage", "battery_current", "battery_percent",
                 "internet"],
                self.timeline_files, self.timeline_writers,
            )
            self._open_csv(
                os.path.join(md, "mission_events.csv"),
                ["local_timestamp", "unix_time", "mission_elapsed_sec", "ros_time",
                 "event_type", "detail", "mode", "mode_class",
                 "current_waypoint_seq", "last_reached_waypoint_seq",
                 "groundspeed_mps", "heading_deg", "latitude", "longitude"],
                self.events_files, self.events_writers,
            )
            self._open_csv(
                os.path.join(md, "mission_state_changes.csv"),
                ["local_timestamp", "unix_time", "mission_elapsed_sec",
                 "old_mode", "new_mode", "old_mode_class", "new_mode_class",
                 "connected", "armed", "guided", "manual_input"],
                self.state_files, self.state_writers,
            )
            self._open_csv(
                os.path.join(md, "mission_waypoint_reached.csv"),
                ["local_timestamp", "unix_time", "mission_elapsed_sec", "ros_time",
                 "frame_id", "wp_seq", "mode", "mode_class",
                 "groundspeed_mps", "heading_deg", "latitude", "longitude"],
                self.wp_reached_files, self.wp_reached_writers,
            )
            self._open_csv(
                os.path.join(md, "mission_waypoints.csv"),
                ["dump_local_timestamp", "dump_unix_time", "dump_mission_elapsed_sec",
                 "current_seq", "waypoint_count", "index", "is_current", "autocontinue",
                 "frame", "command", "param1", "param2", "param3", "param4",
                 "x_lat", "y_long", "z_alt"],
                self.waypoints_files, self.waypoints_writers,
            )
            self._open_csv(
                os.path.join(md, "mission_statustext.csv"),
                ["local_timestamp", "unix_time", "mission_elapsed_sec", "ros_time",
                 "frame_id", "severity", "text", "mode", "mode_class"],
                self.statustext_files, self.statustext_writers,
            )

    # -------------------------------------------------------------------------
    # SENSOR HEADER & PAYLOAD HELPERS
    # -------------------------------------------------------------------------
    def _sensor_header(self, sensor: str) -> list:
        base = ["timestamp", "mission_elapsed_sec"]
        if sensor == "gps":
            return base + ["latitude", "longitude", "altitude", "status", "service"]
        if sensor == "imu":
            return base + ["acc_x", "acc_y", "acc_z"]
        if sensor == "ctd":
            return base + ["depth_m", "temp_c", "cond", "salinity_psu", "density", "soundvel_ms"]
        if sensor == "adcp":
            return base + ["temp_c", "v1_ms", "v2_ms", "v3_ms", "v4_ms"]
        if sensor == "battery":
            return base + ["voltage_v", "current_a", "percentage_percent"]
        return base + ["payload"]

    def _payload_col_names(self, sensor: str) -> list:
        return self._sensor_header(sensor)[2:]

    def _payload_values(self, sensor: str, payload) -> list:
        if payload is None:
            return [""] * len(self._payload_col_names(sensor))
        sv = self._safe
        if sensor == "gps":
            return [sv(payload.get("latitude")), sv(payload.get("longitude")),
                    sv(payload.get("altitude")), sv(payload.get("status")),
                    sv(payload.get("service"))]
        if sensor == "imu":
            return [sv(payload.get("acc_x")), sv(payload.get("acc_y")),
                    sv(payload.get("acc_z"))]
        if sensor == "ctd":
            return [sv(payload.get("depth_m")), sv(payload.get("temp_c")),
                    sv(payload.get("cond")),     sv(payload.get("salinity_psu")),
                    sv(payload.get("density")),  sv(payload.get("soundvel_ms"))]
        if sensor == "adcp":
            return [sv(payload.get("temp_c")), sv(payload.get("v1_ms")),
                    sv(payload.get("v2_ms")),   sv(payload.get("v3_ms")),
                    sv(payload.get("v4_ms"))]
        if sensor == "battery":
            return [sv(payload.get("voltage_v")), sv(payload.get("current_a")),
                    sv(payload.get("percentage_percent"))]
        return [str(payload)]

    def _sync_data_header(self) -> list:
        h = ["sync_time", "mission_elapsed_sec"]
        for s in SENSORS:
            for col in self._payload_col_names(s):
                h.append(f"{s}_{col}")
        return h

    def _sync_diag_header(self) -> list:
        """Header sync_diagnostics.csv — tanpa frame_status, tanpa frame_span."""
        h = ["sync_time", "mission_elapsed_sec"]
        for s in SENSORS:
            h += [f"{s}_time", f"{s}_delay_ms", f"{s}_age_ms", f"{s}_status"]
        return h

    # -------------------------------------------------------------------------
    # SENSOR PUSH (hot path — single thread, no lock needed for buffers)
    # -------------------------------------------------------------------------
    def _push(self, sensor: str, payload: dict, msg_ros_ns: Optional[int] = None):
        if not self.logging_active:
            return

        now_ns  = self._wall_ns()
        elapsed = self._elapsed()

        # Header timestamp dari MAVROS kadang tidak satu clock dengan wall-time Jetson.
        # Jika beda terlalu jauh, jangan dipakai untuk matching karena akan membuat
        # delay palsu sangat besar, misalnya sampai hitungan jam. Fallback ke recv_wall_ns.
        valid_msg_ros_ns = msg_ros_ns
        if msg_ros_ns is not None:
            skew_ms = abs(msg_ros_ns - now_ns) / 1e6
            if skew_ms > self.header_time_max_skew_ms:
                valid_msg_ros_ns = None

        sample = SensorSample(
            sensor=sensor,
            recv_wall_ns=now_ns,
            msg_ros_ns=valid_msg_ros_ns,
            mission_elapsed_sec=elapsed,
            payload=payload,
        )

        self.buffers[sensor].append(sample)
        self.last_rx_ns[sensor]     = now_ns
        self.sample_count[sensor]  += 1

        # Sensor individual CSV: 1 baris per detik per sensor
        slot = now_ns // 1_000_000_000
        if slot != self.sensor_csv_slot[sensor]:
            self.sensor_csv_slot[sensor] = slot
            row = [self._ts_ns(now_ns), f"{elapsed:.3f}"] + self._payload_values(sensor, payload)
            with self._wq_lock:
                self.write_queues[sensor].append(row)

        # Event-driven sync trigger
        if self.enable_event_sync and self.ev_sync_window_ns > 0:
            self.ev_last_sensor_ns[sensor] = now_ns
            self._check_event_sync(now_ns)

    def _flush_write_queues(self, force: bool = False):
        """Kuras write queue ke file sensor CSV. Timer 0.5s atau force saat shutdown."""
        if not self.logging_active and not force:
            return

        with self._wq_lock:
            batches = {s: list(self.write_queues[s]) for s in SENSORS}
            for s in SENSORS:
                self.write_queues[s].clear()

        for sensor, rows in batches.items():
            if not rows:
                continue
            for w in self.sensor_writers[sensor]:
                try:
                    w.writerows(rows)
                except Exception as exc:
                    self._err(f"{sensor} CSV flush error: {exc}")
            self.bytes_written += sum(
                len(",".join(map(str, r)).encode()) for r in rows
            )

    # -------------------------------------------------------------------------
    # SYNC: EVENT-DRIVEN TRIGGER
    # -------------------------------------------------------------------------
    def _check_event_sync(self, now_ns: int):
        """
        Dipanggil setiap kali sensor baru masuk.
        Trigger sync row segera setelah semua sensor fresh dalam window.
        """
        if not self.sync_writers:
            return

        fresh_timeout_ns = int(self.sensor_fresh_timeout_ms * 1e6)

        if not self.ev_sync_pending:
            # Semua sensor punya data baru dalam sensor_fresh_timeout_ms?
            all_fresh = all(
                self.ev_last_sensor_ns[s] > 0
                and (now_ns - self.ev_last_sensor_ns[s]) <= fresh_timeout_ns
                for s in SENSORS
            )
            if all_fresh:
                self.ev_sync_pending = True
                self.ev_trigger_ns   = now_ns
        else:
            # Tunggu event_sync_window_ms berlalu sejak trigger pertama
            if (now_ns - self.ev_trigger_ns) >= self.ev_sync_window_ns:
                self.ev_sync_pending = False
                self.ev_trigger_ns   = 0
                self._do_write_sync_row(now_ns, self._aligned_sync_target_ns(now_ns))

    # -------------------------------------------------------------------------
    # SYNC: TIMER FALLBACK
    # -------------------------------------------------------------------------
    def _sync_timer_cb(self):
        """
        Timer fallback sync.

        Target sync_time dikembalikan ke slot waktu bulat:
            1 Hz -> HH:MM:SS.000

        Tidak ada sync_output_delay.
        Event-driven sync dan timer fallback berbagi target detik yang sama,
        sehingga synchronized_log.csv kembali rapi di detik 00.
        """
        if not self.logging_active or not self.sync_writers:
            return
        if not self.enable_timer_fallback:
            return

        now_ns = self._wall_ns()
        target_ns = self._aligned_sync_target_ns(now_ns)

        if target_ns <= self.last_sync_target_ns:
            self.ev_sync_pending = False
            return

        # Watchdog: jika event sync pending terlalu lama, reset dan biarkan timer fallback.
        if self.ev_sync_pending and (now_ns - self.ev_trigger_ns) > 2 * self.ev_sync_window_ns:
            self.ev_sync_pending = False

        self._do_write_sync_row(now_ns, target_ns)

    # -------------------------------------------------------------------------
    # SYNC: CORE ROW WRITER
    # -------------------------------------------------------------------------
    def _aligned_sync_target_ns(self, now_ns: int) -> int:
        """
        Kembalikan target sync_time pada slot periode bulat.
        Untuk sync_rate_hz=1.0, nanosecond selalu 000000000.
        """
        if self.sync_period_ns <= 0:
            return now_ns
        return (now_ns // self.sync_period_ns) * self.sync_period_ns

    def _sample_ref_ns(self, sample: Optional[SensorSample]) -> Optional[int]:
        """
        Timestamp referensi untuk matching.
        Prioritas:
          1. msg/header timestamp jika tersedia
          2. receive wall time jika tidak ada header timestamp
        """
        if sample is None:
            return None
        return sample.msg_ros_ns if sample.msg_ros_ns is not None else sample.recv_wall_ns

    def _nearest_sample(self, sensor: str, target_ns: int) -> Optional[SensorSample]:
        buf = self.buffers[sensor]
        if not buf:
            return None
        return min(buf, key=lambda s: abs(self._sample_ref_ns(s) - target_ns))

    def _do_write_sync_row(self, now_ns: int, target_ns: Optional[int] = None):
        """
        Inti penulisan satu baris ke synchronized_log.csv dan sync_diagnostics.csv.

        Desain:
          - sync_time dikunci ke slot waktu bulat/detik 00.
          - nearest sample dipilih terhadap sync_time tersebut.
          - Status tetap per sensor: valid / stale / missing.
          - Tidak ada frame_status, frame_span_ms, valid_sensor_count, atau partial.
        """
        if not self.logging_active or not self.sync_writers:
            return

        if target_ns is None:
            target_ns = self._aligned_sync_target_ns(now_ns)

        # Deduplication berbasis sync target, bukan wall write time.
        if target_ns <= self.last_sync_target_ns:
            return

        samples = {s: self._nearest_sample(s, target_ns) for s in SENSORS}

        deltas_ms = {}
        ages_ms   = {}
        statuses  = {}
        sensor_ts = {}
        any_missing = False

        for s in SENSORS:
            smp = samples[s]
            if smp is None:
                deltas_ms[s] = math.nan
                ages_ms[s]   = math.nan
                statuses[s]  = "missing"
                sensor_ts[s] = ""
                self.sync_stats[s].missing += 1
                any_missing = True
                continue

            ref = self._sample_ref_ns(smp)
            delta = (ref - target_ns) / 1e6
            age   = (now_ns - smp.recv_wall_ns) / 1e6

            deltas_ms[s] = delta
            ages_ms[s]   = age
            sensor_ts[s] = self._ts_ns(ref)

            ss = self.sync_stats[s]
            ss.delta_ms.append(abs(delta))
            ss.age_ms.append(age)

            if abs(delta) <= self.sync_tolerance_ms:
                statuses[s] = "valid"
                ss.valid   += 1
            else:
                statuses[s] = "stale"
                ss.stale   += 1

        if any_missing and not self.write_rows_with_missing:
            return

        self.last_sync_target_ns = target_ns
        self.last_sync_write_wall_ns = now_ns
        self.sync_total += 1

        ts_str = self._ts_ns(target_ns)
        el_str = f"{self._elapsed():.3f}"

        data_row = [ts_str, el_str]
        for s in SENSORS:
            data_row += self._payload_values(
                s, samples[s].payload if samples[s] else None
            )

        diag_row = [ts_str, el_str]
        for s in SENSORS:
            smp = samples[s]
            if smp is None:
                diag_row += ["", "", "", "missing"]
            else:
                diag_row += [
                    sensor_ts[s],
                    f"{deltas_ms[s]:.3f}",
                    f"{ages_ms[s]:.3f}",
                    statuses[s],
                ]

        for w in self.sync_writers:
            try:
                w.writerow(data_row)
            except Exception as exc:
                self._err(f"sync write error: {exc}")

        for w in self.diag_writers:
            try:
                w.writerow(diag_row)
            except Exception as exc:
                self._err(f"diag write error: {exc}")

        self.bytes_written += (
            len(",".join(map(str, data_row)).encode())
            + len(",".join(map(str, diag_row)).encode())
        )

    # -------------------------------------------------------------------------
    # SYNC QUALITY SUMMARY — tanpa frame_valid/partial/missing
    # -------------------------------------------------------------------------
    def _write_sync_quality_summary(self):
        for path in self.sync_quality_paths:
            try:
                with open(path, "w", newline="") as f:
                    w = csv.writer(f)
                    # Baris konfigurasi
                    w.writerow(["key", "value"])
                    w.writerow(["sync_row_count",          self.sync_total])
                    w.writerow(["sync_rate_hz",            self.sync_rate_hz])
                    w.writerow(["sync_tolerance_ms",       self.sync_tolerance_ms])
                    w.writerow(["header_time_max_skew_ms", self.header_time_max_skew_ms])
                    w.writerow(["event_sync_window_ms",    self.event_sync_window_ms])
                    w.writerow(["sensor_fresh_timeout_ms", self.sensor_fresh_timeout_ms])
                    w.writerow([])
                    # Per-sensor detail
                    w.writerow([
                        "sensor", "samples_received",
                        "valid_count", "stale_count", "missing_count",
                        "valid_percent", "stale_percent", "missing_percent",
                        "delta_abs_mean_ms", "delta_abs_median_ms", "delta_abs_max_ms",
                        "age_mean_ms", "age_median_ms", "age_max_ms",
                    ])
                    for s in SENSORS:
                        ss  = self.sync_stats[s]
                        tot = ss.valid + ss.stale + ss.missing
                        vp  = ss.valid   / tot * 100 if tot > 0 else 0.0
                        sp  = ss.stale   / tot * 100 if tot > 0 else 0.0
                        mp  = ss.missing / tot * 100 if tot > 0 else 0.0
                        dl  = list(ss.delta_ms)
                        ag  = list(ss.age_ms)
                        w.writerow([
                            s, self.sample_count[s],
                            ss.valid, ss.stale, ss.missing,
                            f"{vp:.1f}", f"{sp:.1f}", f"{mp:.1f}",
                            f"{_mean(dl):.3f}"   if dl else "",
                            f"{_median(dl):.3f}" if dl else "",
                            f"{max(dl):.3f}"     if dl else "",
                            f"{_mean(ag):.3f}"   if ag else "",
                            f"{_median(ag):.3f}" if ag else "",
                            f"{max(ag):.3f}"     if ag else "",
                        ])
            except Exception as exc:
                self._err(f"sync_quality_summary failed: {exc}")

    # -------------------------------------------------------------------------
    # SENSOR CALLBACKS
    # -------------------------------------------------------------------------
    def _gps_cb(self, msg):
        self.gps_lat     = msg.latitude
        self.gps_lon     = msg.longitude
        self.gps_alt     = msg.altitude
        self.gps_status  = msg.status.status
        self.gps_service = msg.status.service
        self._push("gps", {
            "latitude":  msg.latitude,
            "longitude": msg.longitude,
            "altitude":  msg.altitude,
            "status":    msg.status.status,
            "service":   msg.status.service,
        }, self._header_ns(msg))

    def _imu_cb(self, msg):
        self._push("imu", {
            "acc_x": msg.linear_acceleration.x,
            "acc_y": msg.linear_acceleration.y,
            "acc_z": msg.linear_acceleration.z,
        }, self._header_ns(msg))

    def _ctd_cb(self, msg):
        d = list(msg.data)
        self._push("ctd", {
            "depth_m":     d[0] if len(d) > 0 else math.nan,
            "temp_c":      d[1] if len(d) > 1 else math.nan,
            "cond":        d[2] if len(d) > 2 else math.nan,
            "salinity_psu": d[3] if len(d) > 3 else math.nan,
            "density":     d[4] if len(d) > 4 else math.nan,
            "soundvel_ms": d[5] if len(d) > 5 else math.nan,
        }, None)  # CTD tidak punya header ROS; fallback ke recv_wall_ns

    def _adcp_cb(self, msg):
        d = list(msg.data)
        if len(d) < 5:
            if not self.adcp_warn_reported:
                self._warn(f"ADCP data kurang: expected 5, got {len(d)}")
                self.adcp_warn_reported = True
            payload = {k: math.nan for k in ["temp_c", "v1_ms", "v2_ms", "v3_ms", "v4_ms"]}
        else:
            payload = {
                "temp_c": d[0], "v1_ms": d[1],
                "v2_ms":  d[2], "v3_ms": d[3], "v4_ms": d[4],
            }
            self.adcp_warn_reported = False
        self._push("adcp", payload, None)  # ADCP tidak punya header ROS

    def _battery_cb(self, msg):
        pct = float(msg.percentage)
        if not math.isnan(pct) and pct <= 1.0:
            pct *= 100.0
        self.batt_voltage  = msg.voltage
        self.batt_current  = msg.current
        self.batt_percent  = pct

        # Cache terakhir tetap hidup walau logger belum aktif.
        self.latest_batt_voltage = msg.voltage
        self.latest_batt_current = msg.current
        self.latest_batt_percent = pct

        # Jika saat ARM nilai start battery belum ada, isi dari sample battery pertama
        # yang diterima saat mission aktif.
        if self.logging_active and (not isinstance(self.batt_start, float) or math.isnan(self.batt_start)):
            self.batt_start = pct

        self._push("battery", {
            "voltage_v":           msg.voltage,
            "current_a":           msg.current,
            "percentage_percent":  pct,
        }, self._header_ns(msg))

    # -------------------------------------------------------------------------
    # MAVROS CALLBACKS
    # -------------------------------------------------------------------------
    def _mavros_state_cb(self, msg):
        mode_class = self._classify_mode(msg.mode)
        prev_armed = self.mav_armed

        self.mav_connected  = msg.connected
        self.mav_armed      = msg.armed
        self.mav_guided     = msg.guided
        self.mav_manual     = msg.manual_input
        self.mav_mode       = msg.mode
        self.mav_mode_class = mode_class
        self.mav_sys_status = msg.system_status

        if msg.armed and not prev_armed:
            self._start_session(msg)
        elif not msg.armed and prev_armed and self.logging_active:
            self._write_event("DISARM",
                              f"mode={msg.mode}, connected={msg.connected}")
            self._stop_session(f"mode={msg.mode}, armed={msg.armed}")

        if self.logging_active and msg.mode != self.prev_mode:
            self._write_state_change(
                self.prev_mode, msg.mode,
                self.prev_mode_class, mode_class,
            )
            self._write_event("MODE_CHANGE",
                              f"{self.prev_mode} -> {msg.mode}")

        self.prev_mode       = msg.mode
        self.prev_mode_class = mode_class

    def _vfr_hud_cb(self, msg):
        self.vfr_groundspeed = msg.groundspeed
        self.vfr_airspeed    = msg.airspeed
        self.vfr_heading     = msg.heading
        self.vfr_throttle    = msg.throttle
        self.vfr_altitude    = msg.altitude
        self.vfr_climb       = msg.climb

    def _waypoints_cb(self, msg):
        self.wp_last_msg = msg
        self.wp_current  = int(msg.current_seq)
        self.wp_count    = len(msg.waypoints)
        if not self.logging_active:
            return
        sig = self._wp_signature(msg)
        if sig == self.wp_signature:
            return
        self.wp_signature = sig
        self._dump_waypoints(msg)
        self._write_event("WAYPOINT_LIST_UPDATED",
                          f"count={len(msg.waypoints)}, current_seq={msg.current_seq}")

    def _wp_reached_cb(self, msg):
        self.wp_last_reached = int(msg.wp_seq)
        if not self.logging_active:
            return
        ros_time = float(msg.header.stamp.sec) + float(msg.header.stamp.nanosec) * 1e-9
        row = [
            self._ts_now(), f"{self._unix_now():.6f}",
            f"{self._elapsed():.3f}", f"{ros_time:.9f}",
            msg.header.frame_id, int(msg.wp_seq),
            self.mav_mode, self.mav_mode_class,
            self._safe(self.vfr_groundspeed), self._safe(self.vfr_heading),
            self._safe(self.gps_lat), self._safe(self.gps_lon),
        ]
        for w in self.wp_reached_writers:
            w.writerow(row)
        self._write_event("WAYPOINT_REACHED", f"wp_seq={msg.wp_seq}", ros_time)

    def _statustext_cb(self, msg):
        if not self.logging_active:
            return
        ros_time = float(msg.header.stamp.sec) + float(msg.header.stamp.nanosec) * 1e-9
        text = str(msg.text)
        row = [
            self._ts_now(), f"{self._unix_now():.6f}",
            f"{self._elapsed():.3f}", f"{ros_time:.9f}",
            msg.header.frame_id, int(msg.severity), text,
            self.mav_mode, self.mav_mode_class,
        ]
        for w in self.statustext_writers:
            w.writerow(row)
        tu = text.upper()
        if any(k in tu for k in ("RTL", "RTH", "RETURN", "FAILSAFE")):
            self._write_event("AUTOPILOT_STATUS_ALERT", text, ros_time)

    # -------------------------------------------------------------------------
    # MISSION WRITE HELPERS
    # -------------------------------------------------------------------------
    def _write_event(self, event_type: str, detail: str, ros_time=None):
        if not self.logging_active:
            return
        self.event_count += 1
        eu, du = event_type.upper(), str(detail).upper()
        if "RETURN" in eu or any(k in du for k in ("RTL", "RTH", "RETURN")):
            self.rtl_detected = True
        if "FAILSAFE" in eu or "FAILSAFE" in du:
            self.failsafe_detected = True

        row = [
            self._ts_now(), f"{self._unix_now():.6f}",
            f"{self._elapsed():.3f}",
            "" if ros_time is None else f"{ros_time:.9f}",
            event_type, detail,
            self.mav_mode, self.mav_mode_class,
            self.wp_current, self.wp_last_reached,
            self._safe(self.vfr_groundspeed), self._safe(self.vfr_heading),
            self._safe(self.gps_lat), self._safe(self.gps_lon),
        ]
        for w in self.events_writers:
            w.writerow(row)

        line = (
            f"{self._ts_now()} | t={self._elapsed():.1f}s | "
            f"{event_type}: {detail} | mode={self.mav_mode} | "
            f"wp={self.wp_current} | speed={self._safe(self.vfr_groundspeed)} | "
            f"pos={self._safe(self.gps_lat)},{self._safe(self.gps_lon)}\n"
        )
        for f in self.mission_log_files:
            f.write(line)

    def _write_state_change(self, old_mode, new_mode, old_class, new_class):
        if not self.logging_active:
            return
        row = [
            self._ts_now(), f"{self._unix_now():.6f}",
            f"{self._elapsed():.3f}",
            old_mode, new_mode, old_class, new_class,
            self.mav_connected, self.mav_armed, self.mav_guided, self.mav_manual,
        ]
        for w in self.state_writers:
            w.writerow(row)

    def _wp_signature(self, msg) -> str:
        parts = [str(msg.current_seq), str(len(msg.waypoints))]
        for wp in msg.waypoints:
            parts.append(
                f"{wp.frame}:{wp.command}:{wp.x_lat:.8f}:{wp.y_long:.8f}:{wp.z_alt:.3f}"
            )
        return "|".join(parts)

    def _dump_waypoints(self, msg):
        if not self.logging_active:
            return
        ts  = self._ts_now()
        un  = f"{self._unix_now():.6f}"
        el  = f"{self._elapsed():.3f}"
        cnt = len(msg.waypoints)
        for idx, wp in enumerate(msg.waypoints):
            row = [ts, un, el, int(msg.current_seq), cnt, idx,
                   wp.is_current, wp.autocontinue, wp.frame, wp.command,
                   wp.param1, wp.param2, wp.param3, wp.param4,
                   wp.x_lat, wp.y_long, wp.z_alt]
            for w in self.waypoints_writers:
                w.writerow(row)

    # -------------------------------------------------------------------------
    # TIMELINE + METRICS
    # -------------------------------------------------------------------------
    def _log_timeline(self):
        if not self.logging_active:
            return
        slot = int(time.time())
        if slot == self.last_timeline_slot:
            return
        self.last_timeline_slot = slot
        self.last_timeline_time = time.time()

        gs = self.vfr_groundspeed
        if isinstance(gs, float) and not math.isnan(gs):
            self.max_speed      = max(self.max_speed, gs)
            self.speed_sum     += gs
            self.speed_samples += 1
        self._update_distance()

        inet = self._inet_text(self._get_internet_status())

        tl_row = [
            self._ts_now(), f"{self._unix_now():.6f}", f"{self._elapsed():.3f}",
            self.mav_connected, self.mav_armed, self.mav_guided, self.mav_manual,
            self.mav_mode, self.mav_mode_class, self.mav_sys_status,
            self.wp_current, self.wp_last_reached, self.wp_count,
            self._safe(self.vfr_groundspeed), self._safe(self.vfr_airspeed),
            self._safe(self.vfr_heading),  self._safe(self.vfr_throttle),
            self._safe(self.vfr_altitude), self._safe(self.vfr_climb),
            self._safe(self.gps_lat), self._safe(self.gps_lon), self._safe(self.gps_alt),
            self._safe(self.gps_status), self._safe(self.gps_service),
            self._safe(self.batt_voltage), self._safe(self.batt_current),
            self._safe(self.batt_percent),
            inet,
        ]
        for w in self.timeline_writers:
            w.writerow(tl_row)

        note = ""
        if self.mav_mode_class == "RETURN_HOME":
            note = "RETURN HOME / RTL"
        elif self.wp_last_reached >= 0:
            note = f"Last WP: {self.wp_last_reached}"

        rd_row = [
            self._ts_now(), f"{self._elapsed():.1f}",
            "ACTIVE" if self.mav_armed else "STANDBY",
            self.mav_mode, self.mav_mode_class,
            self.wp_current, self.wp_last_reached, self.wp_count,
            self._safe(self.vfr_groundspeed), self._safe(self.vfr_heading),
            self._safe(self.vfr_throttle),
            self._safe(self.gps_lat), self._safe(self.gps_lon),
            self._safe(self.gps_alt), self._safe(self.batt_percent),
            inet, note,
        ]
        for w in self.readable_writers:
            w.writerow(rd_row)

        self._write_inet_status_row()

    def _log_metrics(self):
        now     = time.time()
        elapsed = max(now - self.last_metrics_time, 0.001)

        write_speed         = self.bytes_written / elapsed
        cpu                 = psutil.cpu_percent(interval=None)
        ram                 = psutil.virtual_memory().percent
        temp                = self._read_jetson_temp()
        temp_str            = f"{temp:.2f}" if temp is not None else ""
        self.bytes_written  = 0
        self.last_metrics_time = now

        if not self.logging_active or not self.metrics_writers:
            return

        row = [
            self._ts_current_second(),
            f"{write_speed:.2f}", f"{cpu:.2f}", f"{ram:.2f}", temp_str,
            f"{self._sensor_hz('gps'):.2f}",
            f"{self._sensor_hz('imu'):.2f}",
            f"{self._sensor_hz('ctd'):.2f}",
            f"{self._sensor_hz('adcp'):.2f}",
            f"{self._sensor_hz('battery'):.2f}",
            f"{self._timeline_hz():.2f}",
        ]
        for w in self.metrics_writers:
            try:
                w.writerow(row)
            except Exception as exc:
                self._err(f"metrics write error: {exc}")

    def _sensor_hz(self, sensor: str) -> float:
        ns = self.last_rx_ns.get(sensor, 0)
        if ns <= 0:
            return 0.0
        return 1.0 if (self._wall_ns() - ns) / 1e9 <= 1.5 else 0.0

    def _timeline_hz(self) -> float:
        if self.last_timeline_time <= 0.0:
            return 0.0
        return 1.0 if (time.time() - self.last_timeline_time) <= 1.5 else 0.0

    def _read_jetson_temp(self) -> Optional[float]:
        preferred = [
            "CPU-therm", "cpu-therm", "Tdiode_tegra", "Tboard_tegra",
            "GPU-therm", "gpu-therm", "SOC0-therm", "soc0-therm",
        ]
        valid = []
        for zone in glob.glob("/sys/class/thermal/thermal_zone*"):
            try:
                with open(os.path.join(zone, "type")) as f:
                    zt = f.read().strip()
                with open(os.path.join(zone, "temp")) as f:
                    raw = float(f.read().strip())
                tc = raw / 1000.0 if abs(raw) > 1000.0 else raw
                valid.append((zt, tc))
            except Exception:
                continue
        if not valid:
            return None
        for pref in preferred:
            for zt, tc in valid:
                if zt == pref:
                    return tc
        return valid[0][1]

    def _update_distance(self):
        lat, lon = self.gps_lat, self.gps_lon
        if not isinstance(lat, float) or not isinstance(lon, float):
            return
        if math.isnan(lat) or math.isnan(lon):
            return
        if self.dist_last_lat is None:
            self.dist_last_lat, self.dist_last_lon = lat, lon
            return
        step = haversine_m(self.dist_last_lat, self.dist_last_lon, lat, lon)
        if 0.0 <= step <= 50.0:
            self.distance_m   += step
            self.dist_last_lat = lat
            self.dist_last_lon = lon

    # -------------------------------------------------------------------------
    # INTERNET MONITOR (daemon thread — TCP probe adalah blocking call)
    # -------------------------------------------------------------------------
    def _internet_worker(self):
        while True:
            online = self._tcp_probe(INTERNET_HOST, INTERNET_PORT, INTERNET_TIMEOUT_SEC)
            now    = time.time()

            with self._internet_lock:
                prev = self._internet_online

                if prev is None:
                    self._internet_online = online
                    if not online:
                        self._internet_lost_since = now
                        self._internet_lost_count += 1
                        self._inet_event("INTERNET OFFLINE sejak awal logging")
                    else:
                        self._inet_event("INTERNET ONLINE")
                elif prev and not online:
                    self._internet_online     = False
                    self._internet_lost_since = now
                    self._internet_lost_count += 1
                    self._inet_event("INTERNET OFFLINE")
                elif not prev and online:
                    down = 0.0
                    if self._internet_lost_since is not None:
                        down = now - self._internet_lost_since
                        self._internet_total_down_sec += down
                    self._internet_online     = True
                    self._internet_lost_since = None
                    self._inet_event(f"INTERNET ONLINE lagi setelah {down:.1f}s")

            time.sleep(INTERNET_CHECK_SEC)

    @staticmethod
    def _tcp_probe(host: str, port: int, timeout: float) -> bool:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except Exception:
            return False

    def _get_internet_status(self):
        with self._internet_lock:
            return self._internet_online

    def _get_internet_stats(self):
        with self._internet_lock:
            total = self._internet_total_down_sec
            if self._internet_online is False and self._internet_lost_since is not None:
                total += time.time() - self._internet_lost_since
            return (self._internet_online, self._internet_lost_count, total)

    def _inet_text(self, status) -> str:
        if status is None:
            return "UNKNOWN"
        return "ONLINE" if status else "OFFLINE"

    def _inet_event(self, text: str):
        """Tulis event internet. Dipanggil dari thread inet_monitor — aman karena GIL."""
        if not self.logging_active:
            return
        line = f"{self._ts_now()} | t={self._elapsed():.1f}s | {text}\n"
        for f in self.inet_event_files:
            try:
                f.write(line)
                f.flush()
            except Exception:
                pass

    def _write_inet_status_row(self):
        if not self.inet_status_writers:
            return
        slot = int(time.time())
        if slot == self.last_internet_slot:
            return
        self.last_internet_slot = slot

        status, lost, total_down = self._get_internet_stats()
        elapsed = self._elapsed()
        avail   = max(0.0, (elapsed - total_down) / elapsed * 100.0) if elapsed > 0 else 0.0

        row = [
            self._ts_now(), f"{elapsed:.1f}",
            self._inet_text(status),
            lost,
            f"{total_down:.1f}",
            f"{avail:.1f}",
            "",
        ]
        for w in self.inet_status_writers:
            try:
                w.writerow(row)
            except Exception:
                pass

    # -------------------------------------------------------------------------
    # MISSION SUMMARY
    # -------------------------------------------------------------------------
    def _write_mission_summary(self, reason: str, end_time, elapsed: float):
        avg_speed = (self.speed_sum / self.speed_samples) if self.speed_samples > 0 else 0.0
        self.batt_end = self.batt_percent
        _, lost, total_down = self._get_internet_stats()
        avail = max(0.0, (elapsed - total_down) / elapsed * 100.0) if elapsed > 0 else 0.0

        for bp in self.base_paths:
            path = os.path.join(bp, "mission_summary.txt")
            try:
                with open(path, "w") as f:
                    f.write("SEANO Mission Summary\n=====================\n\n")
                    f.write(f"Mission ID            : {self.mission_id}\n")
                    f.write(f"Start time            : {self.start_time_obj}\n")
                    f.write(f"End time              : {end_time}\n")
                    f.write(f"Duration              : {elapsed:.1f}s ({elapsed/60:.2f} min)\n")
                    f.write(f"Stop reason           : {reason}\n\n")
                    f.write("Final state\n-----------\n")
                    f.write(f"Connected             : {self.mav_connected}\n")
                    f.write(f"Armed                 : {self.mav_armed}\n")
                    f.write(f"Mode                  : {self.mav_mode} ({self.mav_mode_class})\n")
                    f.write(f"Current WP            : {self.wp_current}\n")
                    f.write(f"Last reached WP       : {self.wp_last_reached}\n")
                    f.write(f"WP count              : {self.wp_count}\n\n")
                    f.write("Movement\n--------\n")
                    f.write(f"Max speed             : {self.max_speed:.3f} m/s\n")
                    f.write(f"Average speed         : {avg_speed:.3f} m/s\n")
                    f.write(f"Estimated distance    : {self.distance_m:.2f} m\n")
                    f.write(f"Last lat/lon          : {self._safe(self.gps_lat)} / {self._safe(self.gps_lon)}\n")
                    f.write(f"Last heading          : {self._safe(self.vfr_heading)} deg\n\n")
                    f.write("Battery\n-------\n")
                    f.write(f"Start battery         : {self._safe(self.batt_start)} %\n")
                    f.write(f"End battery           : {self._safe(self.batt_end)} %\n")
                    f.write(f"Last voltage          : {self._safe(self.batt_voltage)} V\n")
                    f.write(f"Last current          : {self._safe(self.batt_current)} A\n\n")
                    f.write("Internet\n--------\n")
                    f.write(
                        f"Final status          : {self._inet_text(self._get_internet_status())}\n"
                    )
                    f.write(f"Lost count            : {lost}\n")
                    f.write(f"Total down            : {total_down:.1f}s\n")
                    f.write(f"Availability          : {avail:.1f}%\n\n")
                    f.write("Events\n------\n")
                    f.write(f"Event count           : {self.event_count}\n")
                    f.write(f"RTL detected          : {self.rtl_detected}\n")
                    f.write(f"Failsafe detected     : {self.failsafe_detected}\n\n")
                    f.write("Recommended files\n-----------------\n")
                    f.write("1. sensor/synchronized_log.xlsx (jika enable_xlsx_export=True)\n")
                    f.write("2. sensor/synchronized_log.csv\n")
                    f.write("3. sensor/sync_diagnostics.csv\n")
                    f.write("4. sensor/sync_quality_summary.csv\n")
                    f.write("5. system_metrics.csv\n")
                    f.write("6. mission/mission_readable.csv\n")
                    f.write("7. mission/mission_events_readable.log\n")
            except Exception as exc:
                self._err(f"mission_summary write failed: {exc}")

    # -------------------------------------------------------------------------
    # XLSX EXPORT — no tmp file in mission folder
    # -------------------------------------------------------------------------
    def _export_xlsx_all(self):
        """
        Export XLSX synchronous setelah semua CSV ditutup.

        File sementara dibuat sepenuhnya di /tmp memakai TemporaryDirectory.
        Tidak membuat .xlsx_tmp_* atau .seano_xlsx_in_progress di folder mission/sensor.
        """
        if not self.enable_xlsx:
            return

        for bp in self.base_paths:
            sd = os.path.join(bp, "sensor")
            if not os.path.isdir(sd):
                continue

            sync_csv = os.path.join(sd, "synchronized_log.csv")
            diag_csv = os.path.join(sd, "sync_diagnostics.csv")
            qual_csv = os.path.join(sd, "sync_quality_summary.csv")

            if not os.path.exists(sync_csv):
                self._err(f"XLSX export skipped: missing {sync_csv}")
                continue

            xlsx_final = os.path.join(sd, "synchronized_log.xlsx")

            try:
                with tempfile.TemporaryDirectory(dir="/tmp", prefix="seano_xlsx_") as tmpdir:
                    sources = [("Sync_Data", sync_csv)]

                    for sname, tmp_csv in self._build_per_sensor_csvs(tmpdir, sync_csv):
                        sources.append((sname, tmp_csv))

                    if os.path.exists(diag_csv):
                        sources.append(("Sync_Diagnostics", diag_csv))
                    if os.path.exists(qual_csv):
                        sources.append(("Sync_Quality", qual_csv))

                    tmp_xlsx = os.path.join(tmpdir, "synchronized_log.xlsx")
                    write_xlsx(tmp_xlsx, sources)

                    # Copy final langsung ke sensor/. Tidak ada file tmp di mission folder.
                    shutil.copyfile(tmp_xlsx, xlsx_final)

                self.get_logger().info(f"XLSX exported: {xlsx_final}")

            except Exception as exc:
                self._err(f"XLSX export failed: {exc}")

    def _build_per_sensor_csvs(self, tmpdir: str, sync_csv: str) -> list:
        """Buat CSV per sensor di tmpdir (/tmp), bukan di folder mission."""
        if not os.path.exists(sync_csv):
            return []

        sheet_names = {
            "gps":     "GPS_Sync",
            "imu":     "IMU_Sync",
            "ctd":     "CTD_Sync",
            "adcp":    "ADCP_Sync",
            "battery": "Battery_Sync",
        }

        with open(sync_csv, "r", newline="", encoding="utf-8") as f:
            reader     = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            rows       = list(reader)

        base_cols = ["sync_time", "mission_elapsed_sec"]
        result    = []

        for s in SENSORS:
            s_cols = [c for c in fieldnames if c.startswith(f"{s}_")]
            if not s_cols:
                continue

            tmp = os.path.join(tmpdir, f"sensor_{s}.csv")
            with open(tmp, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(base_cols + [c[len(s) + 1:] for c in s_cols])
                for row in rows:
                    w.writerow(
                        [row.get(c, "") for c in base_cols]
                        + [row.get(c, "") for c in s_cols]
                    )

            result.append((sheet_names.get(s, f"{s.upper()}_Sync"), tmp))

        return result

    # -------------------------------------------------------------------------
    # CLOSE FILES
    # -------------------------------------------------------------------------
    def _close_all_files(self):
        all_files = []
        for s in SENSORS:
            all_files += self.sensor_files[s]
        all_files += (
            self.sync_files + self.diag_files + self.metrics_files
            + self.timeline_files + self.readable_files + self.events_files
            + self.state_files + self.wp_reached_files + self.waypoints_files
            + self.statustext_files + self.inet_status_files
            + self.inet_event_files + self.mission_log_files
        )
        for f in all_files:
            try:
                f.flush()
                f.close()
            except Exception:
                pass

    def destroy_node(self):
        if self.logging_active:
            self._stop_session("node shutdown")
        else:
            self._close_all_files()
        try:
            os.sync()
        except Exception:
            pass
        super().destroy_node()


# =============================================================================
# ENTRY POINT
# =============================================================================

def main(args=None):
    rclpy.init(args=args)
    node = SeanoLogger()
    executor = SingleThreadedExecutor()
    executor.add_node(node)
    try:
        executor.spin()
    except KeyboardInterrupt:
        pass
    finally:
        executor.remove_node(node)
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()