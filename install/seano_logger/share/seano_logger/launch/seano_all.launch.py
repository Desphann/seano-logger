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
        'image_topic': '/seano/camera/image_raw_reliable',
        'image_reliability': 'reliable',
        'external_mount_point': '/mnt/seano/SEANO_SSD',
        'local_mount_point': '/home/seano/Documents/SEANO_logs',
        'output_fps': 4.0,
        'codec': 'mp4v',
        'segment_seconds': 300.0,
        'record_every_n_frames': 1,
        'require_external_on_mission': False,
    }]
),
    ])
