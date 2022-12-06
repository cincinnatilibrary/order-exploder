#!/bin/bash

# note: add this via visudo...
# plchuser ALL=(ALL) NOPASSWD:/usr/bin/systemctl restart datasette.service, /usr/bin/systemctl stop datasette.service, /usr/bin/systemctl start datasette.service, /usr/bin/systemctl status datasette.service

# note: remember to set the system timezone correctly...
# sudo timedatectl set-timezone America/New_York

# 0 7,11,15,17 * * 1,2,3,4,5	cd /home/plchuser/order-exploder/ ; ./cron_build_current_order_db.sh

cd /home/plchuser/order-exploder/
venv/bin/jupyter-nbconvert --to notebook --execute build_current_order_db.ipynb

sudo /usr/bin/systemctl restart datasette.service
