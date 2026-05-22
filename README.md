# SEANO Logger Launch Guide
Sebelum pindah device, **jangan lupa ubah path SSD** di `logger_node.py`.

Sesuaikan self.external_mount_point dengan lokasi SSD di device yang dipakai.

## Run SEANO Logger
```bash
sudo mount /dev/sda1 /mnt/seano
sudo umount /mnt/seano

colcon build 
source install/setup.bash

sudo systemctl start seano_logger
journalctl -u seano_logger -f
sudo systemctl stop seano_logger
sudo systemctl restart seano_logger
systemctl status seano_logger

sudo systemctl restart seano_logger
journalctl -u seano_logger -f



