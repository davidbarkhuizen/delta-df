#! /usr/bin/env bash

. .venv/bin/activate

declare -a file_names=(
    "etl_input.pkl"
    # "before_determining_bookings.pkl"
    # "bookings_df_before_fin_cols.pkl"
    # "bookings_df_after_fin_cols.pkl"
)

sort_order="core_trip_id,actual_trip_code,trip_id" # ,compass_booking_id

for file_name in "${file_names[@]}"
do
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"
    echo "${file_name}"
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"
    
    python3 delta.py \
        --ref_label='on-prem' \
        --ref_path="/Users/david.barkhuizen/code/delta-df/in/on-prem/${file_name}" \
        --ref_compression='gzip' \
        --target_label='staging' \
        --target_path="/Users/david.barkhuizen/code/delta-df/in/staging/${file_name}" \
        --target_compression='zip' \
        --sort_order="${sort_order}"

    echo
done