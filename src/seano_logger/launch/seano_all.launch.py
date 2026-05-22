from launch import LaunchDescription
from launch_ros.actions import Node


def generate_launch_description():
    return LaunchDescription([
        Node(
            package='seano_sensors',
            executable='gps_reader',
            name='gps_reader',
            output='screen'
        ),
        Node(
            package='seano_sensors',
            executable='imu_reader',
            name='imu_reader',
            output='screen'
        ),
        Node(
            package='seano_sensors',
            executable='ctd_reader',
            name='ctd_reader',
            output='screen'
        ),
        Node(
            package='seano_sensors',
            executable='adcp_reader',
            name='adcp_reader',
            output='screen'
        ),
        Node(
            package='seano_sensors',
            executable='battery_reader',
            name='battery_reader',
            output='screen'
        ),
        Node(
            package='seano_sensors',
            executable='sbes_reader',
            name='sbes_reader',
            output='screen'
        ),
        Node(
            package='seano_logger',
            executable='logger_node',
            name='logger_node',
            output='screen'
        ),
	Node(
    package='seano_sensors',
    executable='video_logger_node',
    name='video_logger_node',
    output='screen',
    parameters=[{
        'external_mount_point': '/mnt/seano/SEANO_SSD',
        'local_mount_point': '/home/seano/Documents/SEANO_logs',

        # kamera kamu maksimum 30 FPS, jangan paksa 60
        'camera_device': '/dev/video0',
        'camera_width': 640,
        'camera_height': 480,
        'camera_fps': 30.0,
        'output_fps': 30.0,
        'use_mjpg': True,

        # satu file penuh per mission
        'segment_seconds': 0.0,

        # tulis ke SSD dan Documents
        'single_target_mode': False,
        'enable_external_logging': True,
        'enable_local_logging': True,

        # jangan bikin mission folder sendiri
        'create_own_mission_folder_if_missing': False,

        # jangan debug/HUD
        'publish_debug_compressed': False,

        'codec': 'mp4v',
        'require_external_on_mission': False,
        'mission_gate_topic': '/mavros/state',
    }]
),
    ])
