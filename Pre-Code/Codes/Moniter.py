import os
import numpy as np
from datetime import datetime, timedelta, timezone
from typing import List, Tuple
import json
import pandas as pd
import time

def read_parameters(json_file: str) -> dict:
    with open(json_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def read_fft_file(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path, sep='\s+', names=["FREQ", "REAL", "IMAGE", "AMP", "PHS"], skiprows=1)

def get_time_window_files(directory: str, start_time: datetime, end_time: datetime, ID: str) -> List[str]:
    files_in_window = []
    if os.path.exists(directory):
        for file in os.listdir(directory):
            if file.endswith(".fft") and ID in file:
                file_timestamp_str = file.split('-')[0]
                file_timestamp = datetime.strptime(file_timestamp_str, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
                if start_time <= file_timestamp <= end_time:
                    files_in_window.append(os.path.join(directory, file))
    return files_in_window

def process_tx_rx(tx_id: str, rx_id: str, fft_directory: str, output_directory: str, stack_time_window: int, frequencies: List[float]):
    now = datetime.now(timezone.utc)
    print(f"Processing TX: {tx_id}, RX: {rx_id} at {now}")
    end_time = now.replace(second=0, microsecond=0)
    start_time = end_time - timedelta(minutes=stack_time_window)

    tx_files = get_time_window_files(fft_directory, start_time, end_time, str(tx_id))
    rx_files = get_time_window_files(fft_directory, start_time, end_time, str(rx_id))
    
    if not tx_files or not rx_files:
        print(f"No valid files found for TX: {tx_id} or RX: {rx_id} in the specified time window.")
        return
    
    tx_data_list, rx_data_list = [], []
    for tx_file, rx_file in zip(tx_files, rx_files):
        print(f"Reading TX file: {tx_file} and RX file: {rx_file}")
        tx_df, rx_df = read_fft_file(tx_file), read_fft_file(rx_file)
        tx_data_list.append(tx_df)
        rx_data_list.append(rx_df)
    
    combined_tx_df = pd.concat(tx_data_list)
    combined_rx_df = pd.concat(rx_data_list)
    
    avg_results = []
    for freq in frequencies:
        tx_rows = combined_tx_df[combined_tx_df['FREQ'] == freq]
        rx_rows = combined_rx_df[combined_rx_df['FREQ'] == freq]
        
        if tx_rows.empty or rx_rows.empty:
            print(f"Frequency {freq} not found in TX or RX data.")
            continue
        
        norm_amp = rx_rows['AMP'].mean() / tx_rows['AMP'].mean()
        phase_diff = rx_rows['PHS'].mean() - tx_rows['PHS'].mean()
        avg_results.append((freq, norm_amp, phase_diff))
    
    avg_df = pd.DataFrame(avg_results, columns=['FREQ', 'Normalized_Amplitude', 'Phase_Difference'])
    output_filepath = os.path.join(output_directory, f"{now.strftime('%Y%m%d%H%M')}-{rx_id}.avg")
    with open(output_filepath, 'w', newline='') as f:
        f.write("#PREQ AMP PHS\n")
        avg_df.to_csv(f, index=False, float_format='%.6f', header=False)
    print(f"Processed average file saved to {output_filepath}")

def main_loop(fft_directory: str, output_directory: str, stack_time_window: int, minutes_of_action: List[int], tx_ids: List[int], rx_ids: List[int], frequencies: List[float]):
    while True:
        now = datetime.now(timezone.utc)
        current_minute = now.minute
        
        if current_minute in minutes_of_action:
            print(f"Processing at: {now}")
            for tx_id in tx_ids:
                for rx_id in rx_ids:
                    process_tx_rx(tx_id, rx_id, fft_directory, output_directory, stack_time_window, frequencies)
        
        time.sleep(60)  # 等待一分钟再检查

if __name__ == "__main__":
    json_file = r'C:\Users\xiaoyu\Desktop\AWS Lambda\preavg.json'
    params = read_parameters(json_file)

    fft_directory = params["FFT_INPUT_PATH"]
    output_directory = params["OUTPUT_PATH"]
    stack_time_window = params["STACK_TIME_WINDOW"]
    minutes_of_action = params["MINUTES_OF_ACTION"]

    tx_ids = params["TX_ID"]
    rx_ids = params["RX_ID"]
    frequencies = params["FREQ_HZ"]

    main_loop(fft_directory, output_directory, stack_time_window, minutes_of_action, tx_ids, rx_ids, frequencies)
