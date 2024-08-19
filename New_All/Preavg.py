import os
from datetime import datetime, timedelta, timezone
from typing import List
import json
import pandas as pd
import time

def read_parameters(json_file: str) -> dict:
    print(f"Reading parameters from {json_file}")
    with open(json_file, 'r', encoding='utf-8') as f:
        params = json.load(f)
    print(f"Parameters loaded: {params}")
    return params

def read_fft_file(file_path: str) -> pd.DataFrame:
    print(f"Reading FFT file from {file_path}")
    df = pd.read_csv(file_path, sep='\s+', names=["FREQ", "REAL", "IMAGE", "AMP", "PHS"], skiprows=1)
    print(f"File {file_path} read successfully with {len(df)} rows.")
    return df

def get_time_window_files(directory: str, start_time: datetime, end_time: datetime, ID: str) -> List[str]:
    print(f"Searching for files in {directory} from {start_time} to {end_time} with ID {ID}")
    files_in_window = []
    if os.path.exists(directory):
        for file in os.listdir(directory):
            if file.endswith(".fft") and ID in file:
                file_timestamp_str = file.split('-')[0]
                file_timestamp = datetime.strptime(file_timestamp_str, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
                if start_time <= file_timestamp <= end_time:
                    files_in_window.append(os.path.join(directory, file))
    print(f"Found {len(files_in_window)} files in the specified time window for ID {ID}.")
    return files_in_window

def process_tx_rx(tx_id: str, rx_id: str, fft_directory: str, output_directory: str, stack_time_window: int, frequencies: List[float]):
    # now = datetime(2024, 6, 18, 6, 30, tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)  # 使用当前UTC时间
    print(f"Processing TX: {tx_id}, RX: {rx_id} at {now}")
    end_time = now.replace(second=0, microsecond=0)
    start_time = end_time - timedelta(minutes=stack_time_window)
    print(f"Time window: {start_time} to {end_time}")

    tx_files = get_time_window_files(fft_directory, start_time, end_time, str(tx_id))
    rx_files = get_time_window_files(fft_directory, start_time, end_time, str(rx_id))
    
    if not tx_files or not rx_files:
        print(f"No valid files found for TX: {tx_id} or RX: {rx_id} in the specified time window.")
        return
    
    if len(tx_files) != len(rx_files):
        print(f"Mismatch in the number of TX and RX files for TX_ID {tx_id} and RX_ID {rx_id}.")
        return
    
    tx_data_list, rx_data_list = [], []
    for tx_file, rx_file in zip(tx_files, rx_files):
        print(f"Reading TX file: {tx_file} and RX file: {rx_file}")
        tx_df, rx_df = read_fft_file(tx_file), read_fft_file(rx_file)
        tx_data_list.append(tx_df)
        rx_data_list.append(rx_df)
    
    combined_tx_df = pd.concat(tx_data_list)
    combined_rx_df = pd.concat(rx_data_list)
    print(f"Combined TX data size: {combined_tx_df.shape}, Combined RX data size: {combined_rx_df.shape}")
    
    avg_results = []
    for freq in frequencies:
        print(f"Processing frequency: {freq}")
        tx_rows = combined_tx_df[combined_tx_df['FREQ'] == freq]
        rx_rows = combined_rx_df[combined_rx_df['FREQ'] == freq]
        
        if tx_rows.empty or rx_rows.empty:
            print(f"Frequency {freq} not found in TX or RX data.")
            continue
        
        norm_amp = rx_rows['AMP'].mean() / tx_rows['AMP'].mean()
        phase_diff = rx_rows['PHS'].mean() - tx_rows['PHS'].mean()
        
        avg_results.append((freq, norm_amp, phase_diff))
        print(f"Frequency {freq} - Normalized Amplitude: {norm_amp}, Phase Difference: {phase_diff}")
    
    avg_df = pd.DataFrame(avg_results, columns=['FREQ', 'Normalized_Amplitude', 'Phase_Difference'])
    output_filepath = os.path.join(output_directory, f"{now.strftime('%Y%m%d%H%M')}-{rx_id}.avg")
    with open(output_filepath, 'w', newline='') as f:
        f.write("#FREQ AMP PHS\n")
        avg_df.to_csv(f, index=False, float_format='%.6f', header=False)
    print(f"Processed average file saved to {output_filepath}")

def main_loop(fft_directory: str, output_directory: str, stack_time_window: int, minutes_of_action: List[int], tx_ids: List[int], rx_ids: List[int], frequencies: List[float]):
    while True:
        # now = datetime(2024, 6, 18, 6, 30, tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        current_minute = now.minute
        
        if current_minute in minutes_of_action:
            print(f"Processing at: {now}")
            for tx_id in tx_ids:
                for rx_id in rx_ids:
                    print(f"Starting process for TX_ID: {tx_id}, RX_ID: {rx_id}")
                    process_tx_rx(tx_id, rx_id, fft_directory, output_directory, stack_time_window, frequencies)
            time.sleep(60)  # 避免在同一分钟内多次执行
        else:
            print(f"Current time {now} not in minutes of action. Waiting for the next action time...")
            time.sleep(30)  # 每10秒检查一次时间

if __name__ == "__main__":
    json_file = r'C:\Users\xiaoyu\Desktop\AWS Lambda\preavg.json'
    params = read_parameters(json_file)

    fft_directory = params["INPUT_PATH"]
    output_directory = params["OUTPUT_PATH"]
    stack_time_window = params["STACK_TIME_WINDOW"]
    minutes_of_action = params["MINUTES_OF_ACTION"]

    tx_ids = params["TX_ID"]
    rx_ids = params["RX_ID"]
    frequencies = params["FREQ_HZ"]

    print("Starting main loop...")
    main_loop(fft_directory, output_directory, stack_time_window, minutes_of_action, tx_ids, rx_ids, frequencies)
