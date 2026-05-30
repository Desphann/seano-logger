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
            executable='ctd_adcp_reader',
            name='ctd_adcp_reader',
            output='screen'
        ),
        Node(
            package='seano_sensors',
            executable='battery_reader',
            name='battery_reader',
            output='screen'
        ),
        Node(
            package='seano_logger',
            executable='logger_node',
            name='logger_node',
            output='screen'
        ),
	Node(
            package="seano_sensors",
            executable="video_logger_node",
            name="video_logger_node",
            output="screen",
),
    ])
