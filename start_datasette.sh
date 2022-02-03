#!/bin/bash

/home/plchuser/output/order-exploder/venv/bin/python3 -m datasette serve /home/plchuser/output/order-exploder/my-app/ -h 127.0.0.1 -p 8014
