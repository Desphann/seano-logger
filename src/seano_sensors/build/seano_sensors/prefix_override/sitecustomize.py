import sys
if sys.prefix == '/usr':
    sys.real_prefix = sys.prefix
    sys.prefix = sys.exec_prefix = '/home/raihan/seano_ws/src/seano_sensors/install/seano_sensors'
