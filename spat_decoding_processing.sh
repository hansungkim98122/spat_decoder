#!/bin/bash
echo "Starting decoding and sorting" 
python ./spat_sorter.py &
wait
echo "Decoding and sorting completed" 
wait
echo "Starting data processing"
python ./spat_data_processor.py &
wait
echo "Data processing completed"