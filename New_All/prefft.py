import os
import time
import numpy as np
from datetime import datetime, timezone
from obspy import read
from typing import List, Tuple
import json
import pandas as pd
from scipy.fft import fft, fftfreq

def read_parameters(json_file: str) -> dict:
    with open(json_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def read_acq_parameters(base_directory: str, ID: str) -> Tuple[float, float]:
    acq_file_path = os.path.join(base_directory, str(ID), 'acq.csv')
    if not os.path.exists(acq_file_path):
        raise FileNotFoundError(f"{acq_file_path} not found")
    
    df = pd.read_csv(acq_file_path)
    row = df[df['ID'] == int(ID)]
    if not row.empty:
        return row['conversion_factor'].values[0], row['sample_interval'].values[0]
    else:
        raise ValueError(f"ID {ID} not found in {acq_file_path}")

def restore_time_series(data: np.ndarray, conversion_factor: float, sample_interval: float) -> Tuple[np.ndarray, float]:
    return data * conversion_factor, 1 / sample_interval

def perform_fft(data, sampling_rate, frequencies, bandwidth=0.1):
    n = len(data)
    T = 1.0 / sampling_rate
    yf = fft(data)
    xf = fftfreq(n, T)[:n//2]
    
    results = []
    for freq in frequencies:
        idx = (np.abs(xf - freq)).argmin()
        phase = np.angle(yf[idx])
        
        nearby_indices = np.where((xf >= freq - bandwidth) & (xf <= freq + bandwidth))[0]
        nearby_real = np.sum(2 * yf[nearby_indices].real / n)
        nearby_imag = np.sum(2 * yf[nearby_indices].imag / n)
        nearby_amplitude = np.sum(2 * np.abs(yf[nearby_indices]) / n)
        
        results.append((freq, nearby_real, nearby_imag, nearby_amplitude, phase))
    
    return results

def generate_filename(file_time: datetime, ID: str) -> str:
    return f"{file_time.strftime('%Y%m%d%H%M')}-{ID}.fft"

def write_fft_file(output_path: str, filename: str, results):
    filepath = os.path.join(output_path, filename)
    with open(filepath, 'w') as f:
        f.write("#FREQ   REAL   IMAGE   AMP   PHS\n")
        for freq, r, i, a, p in results:
            f.write(f"{freq:.6f} {r:.6f} {i:.6f} {a:.6f} {p:.6f}\n")

def get_miniseed_files(directory: str, ID: str) -> List[str]:
    target_directory = os.path.join(directory, str(ID), 'tcp download')
    miniseed_files = []
    if os.path.exists(target_directory):
        for file in os.listdir(target_directory):
            if file.endswith(".miniseed") and ID in file:
                file_path = os.path.join(target_directory, file)
                miniseed_files.append(file_path)
    return miniseed_files

def process_tx_rx(tx_id: str, rx_id: str, base_directory: str, output_directory: str, frequencies: List[float]):
    print(f"Processing TX: {tx_id}, RX: {rx_id}")

    tx_files = get_miniseed_files(base_directory, str(tx_id))
    rx_files = get_miniseed_files(base_directory, str(rx_id))

    if not tx_files:
        print(f"No miniseed files found for TX_ID {tx_id} on this machine.")
        return

    if not rx_files:
        print(f"No miniseed files found for RX_ID {rx_id} on this machine.")
        return

    for tx_file, rx_file in zip(tx_files, rx_files):
        print(f"Reading TX file: {tx_file} and RX file: {rx_file}")
        tx_st, rx_st = read(tx_file), read(rx_file)
        tx_data, rx_data = tx_st[0].data, rx_st[0].data

        tx_coeff, tx_interval = read_acq_parameters(base_directory, str(tx_id))
        rx_coeff, rx_interval = read_acq_parameters(base_directory, str(rx_id))

        tx_series, tx_rate = restore_time_series(tx_data, tx_coeff, tx_interval)
        rx_series, rx_rate = restore_time_series(rx_data, rx_coeff, rx_interval)

        tx_file_time = tx_st[0].stats.starttime.datetime
        rx_file_time = rx_st[0].stats.starttime.datetime
        
        tx_filename = generate_filename(tx_file_time, str(tx_id))
        rx_filename = generate_filename(rx_file_time, str(rx_id))
        
        tx_filepath = os.path.join(output_directory, tx_filename)
        rx_filepath = os.path.join(output_directory, rx_filename)

        # 检查是否已经存在FFT文件
        if os.path.exists(tx_filepath) and os.path.exists(rx_filepath):
            print(f"FFT files for TX: {tx_id} and RX: {rx_id} already exist, skipping processing.")
            continue

        print(f"Performing FFT for TX: {tx_id} and RX: {rx_id}")
        tx_fft_results = perform_fft(tx_series, tx_rate, frequencies)
        rx_fft_results = perform_fft(rx_series, rx_rate, frequencies)
        
        write_fft_file(output_directory, tx_filename, tx_fft_results)
        write_fft_file(output_directory, rx_filename, rx_fft_results)

        print(f"FFT files saved for TX: {tx_id} to {tx_filename} and RX: {rx_id} to {rx_filename}")

def main_loop(base_directory: str, output_directory: str, tx_ids: List[int], rx_ids: List[int], frequencies: List[float]):
    print("Starting continuous processing...")
    processed_tx = set()
    processed_rx = set()
    while True:
        for tx_id in tx_ids:
            if tx_id not in processed_tx:
                tx_files = get_miniseed_files(base_directory, str(tx_id))
                if not tx_files:
                    print(f"No miniseed files found for TX_ID {tx_id} on this machine.")
                    processed_tx.add(tx_id)
        for rx_id in rx_ids:
            if rx_id not in processed_rx:
                rx_files = get_miniseed_files(base_directory, str(rx_id))
                if not rx_files:
                    print(f"No miniseed files found for RX_ID {rx_id} on this machine.")
                    processed_rx.add(rx_id)

        for tx_id in tx_ids:
            for rx_id in rx_ids:
                if tx_id in processed_tx or rx_id in processed_rx:
                    continue
                process_tx_rx(tx_id, rx_id, base_directory, output_directory, frequencies)
        print("Sleeping for 1 minutes before next check...")
        time.sleep(60)  # 每1分钟检查一次

if __name__ == "__main__":
    json_file = r'C:\Users\xiaoyu\Desktop\AWS Lambda\prefft.json'
    params = read_parameters(json_file)

    base_directory = params["INPUT_PATH"]
    output_directory = params["OUTPUT_PATH"]

    tx_ids = params["TX_ID"]
    rx_ids = params["RX_ID"]
    frequencies = params["FREQ_HZ"]  # 从JSON文件中读取频率

    main_loop(base_directory, output_directory, tx_ids, rx_ids, frequencies)
