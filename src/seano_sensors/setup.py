from setuptools import find_packages, setup

package_name = 'seano_sensors'

setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='raihan',
    maintainer_email='raihan@todo.todo',
    description='TODO: Package description',
    license='TODO: License declaration',
    extras_require={
        'test': [
            'pytest',
        ],
    },
    entry_points={
        'console_scripts': [
        'gps_reader = seano_sensors.gps_reader:main',
        'imu_reader = seano_sensors.imu_reader:main',
        'ctd_reader = seano_sensors.ctd_reader:main',
        'adcp_reader = seano_sensors.adcp_reader:main',
        'battery_reader = seano_sensors.battery_reader:main',
        'sbes_reader = seano_sensors.sbes_reader:main',
        'video_logger_node = seano_sensors.video_logger_node:main',
        'mission_state_reader_node = seano_sensors.mission_state_reader_node:main',
        ],
    },
)