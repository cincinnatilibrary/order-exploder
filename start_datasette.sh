#!/bin/bash

/home/plchuser/order-exploder/venv/bin/python3 -m datasette serve /home/plchuser/order-exploder/my-app/ -h 127.0.0.1 -p 8010
