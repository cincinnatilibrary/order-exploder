#!/bin/bash

# 0 7,11,15,17 * * 1,2,3,4,5	cd /home/plchuser/output/order-exploder/ ; ./cron_build_current_order_db.sh

cd /home/plchuser/output/order-exploder/
venv/bin/jupyter-nbconvert --to notebook --execute build_current_order_db.ipynb
