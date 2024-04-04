#! /usr/bin/env bash

. .venv/bin/activate

python3 delta.py \
    'local-dev' '/Users/david.barkhuizen/development/lumos/mount/celery-worker-heavy/etl_input.pkl' \
    'on-prem' '/Users/david.barkhuizen/code/delta-df/in/on-prem/etl_input.pkl'

python3 delta.py \
    'local-dev' '/Users/david.barkhuizen/development/lumos/mount/celery-worker-heavy/before_determining_bookings.pkl' \
    'on-prem' '/Users/david.barkhuizen/code/delta-df/in/on-prem/before_determining_bookings.pkl'