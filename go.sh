#! /usr/bin/env bash

. .venv/bin/activate

declare -a file_names=(
    "etl_input.pkl"
    "before_determining_bookings.pkl"
)

sort_order="main_start_date,kk_date,trip_id,start_date,dossier_code,actual_trip_code,core_trip_code"

for file_name in "${file_names[@]}"
do
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"
    python3 delta.py \
        --ref_label='on-prem' \
        --ref_path="/Users/david.barkhuizen/code/delta-df/in/on-prem/${file_name}" \
        --ref_compression='gzip' \
        --target_label='local-dev' \
        --target_path="/Users/david.barkhuizen/development/lumos/mount/celery-worker-heavy/${file_name}" \
        --target_compression='zip' \
        --sort_order="${sort_order}"
done