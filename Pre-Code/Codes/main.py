import os
import numpy as np
from datetime import datetime, timedelta, timezone
from obspy import read
from typing import List, Tuple
import json
import shutil
import pandas as pd
from tqdm import tqdm
from tqdm.auto import tqdm
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import time

def read_parameters(json_file: str) -> dict:
    with open(json_file, 'r', encoding='utf-8') as f:
        parameters = json.load(f)
    return parameters

def read_acq_parameters(base_directory: str, ID: str) -> Tuple[float, float]:
    acq_file_path = os.path.join(base_directory, str(ID), 'acq.csv')
    if not os.path.exists(acq_file_path):
        raise FileNotFoundError(f"{acq_file_path} not found")
    
    df = pd.read_csv(acq_file_path)
    row = df[df['ID'] == int(ID)]
    if not row.empty:
        conversion_factor = row['conversion_factor'].values[0]
        sample_interval = row['sample_interval'].values[0]
        return conversion_factor, sample_interval
    else:
        raise ValueError(f"ID {ID} not found in {acq_file_path}")

def remove_outliers(data: np.ndarray, threshold: float = 3) -> Tuple[float, np.ndarray]:
    z_scores = np.abs((data - np.mean(data)) / np.std(data))
    filtered_indices = np.where(z_scores < threshold)[0]
    mean_value = np.mean(data[filtered_indices])
    return mean_value, filtered_indices

def detect_stable_segments(data: np.ndarray, window_size: int = 10, threshold: float = 0.01, min_length: int = 10) -> List[int]:
    num_windows = len(data) - window_size + 1
    data_y = np.zeros(num_windows)

    for i in range(num_windows):
        idx = slice(i, i + window_size)
        if (i + window_size) <= len(data) and (i + 2 * window_size) <= len(data):
            idy = slice(i + window_size, i + 2 * window_size)
            data_y[i] = np.mean(data[idy]) - np.mean(data[idx])
        else:
            data_y[i] = np.mean(data[idx])

    add_value = np.mean(data_y)
    data_y = np.concatenate((np.full(window_size, add_value), data_y))

    absnum = np.max(data_y) / 2
    data_y[np.abs(data_y) < absnum] = 0

    peak_sort = []
    for i in range(1, len(data_y) - 1):
        if (data_y[i] - data_y[i - 1]) * (data_y[i + 1] - data_y[i]) < 0:
            peak_sort.append(i)

    peak = []
    for i in range(len(peak_sort) - 1):
        if peak_sort[i + 1] - peak_sort[i] < 20:
            peak.append(peak_sort[i])

    peak_sort = [p for p in peak_sort if p not in peak]

    return peak_sort

def calculate_dc_amp(data: np.ndarray, peak_sort: List[int], Hall_coefficient: float) -> Tuple[List[Tuple[int, int]], np.ndarray]:
    DC_amp = []
    peak_indices = []

    for i in range(len(peak_sort) - 1):
        if peak_sort[i + 1] <= len(data):
            mean_value, indices = remove_outliers(data[peak_sort[i]:peak_sort[i + 1]])
            DC_amp.append(mean_value * Hall_coefficient)
            peak_indices.append((peak_sort[i] + min(indices) + 3, peak_sort[i] + max(indices) - 3))

    DC_amp = np.array(DC_amp)
    DC_amp[np.abs(DC_amp) < (np.max(DC_amp) / 2)] = 0

    return peak_indices, DC_amp

def generate_filename(timestamp: datetime, ID: str, extension: str) -> str:
    timestamp_str = timestamp.strftime("%Y%m%d%H%M")
    return f"{timestamp_str}-{ID}.{extension}"

def write_crt_file(filename: str, peak_indices: List[Tuple[int, int]], DC_amp: np.ndarray, timestamp: datetime, sample_interval: float = 0.001):
    date_time_str = timestamp.strftime('%Y%m%d%H%M')
    seconds_str = timestamp.strftime('%S')

    with open(filename, 'w') as f:
        f.write(f'{date_time_str},{seconds_str},{sample_interval}\n')
        for (start, end), amp in zip(peak_indices, DC_amp):
            f.write(f'{start}, {end}, {amp}\n')

def monitor_new(data: np.ndarray, timestamp: datetime, Hall_coefficient: float, output_directory: str, start_time: datetime, end_time: datetime, TX_ID: str):
    window_size = 10
    peak_sort = detect_stable_segments(data, window_size)
    peak_indices, DC_amp = calculate_dc_amp(data, peak_sort, Hall_coefficient)

    filename = generate_filename(end_time, TX_ID, "crt")
    output_filename = os.path.join(output_directory, filename)
    write_crt_file(output_filename, peak_indices, DC_amp, timestamp)

def find_miniseed_file(directory: str, ID: str) -> str:
    target_directory = os.path.join(directory, ID, 'tcp download')
    if os.path.exists(target_directory):
        for file in os.listdir(target_directory):
            if file.endswith(".miniseed") and ID in file:
                return os.path.join(target_directory, file)
    return None

def get_time_window_files(directory: str, start_time: datetime, end_time: datetime, ID: str) -> List[str]:
    target_directory = os.path.join(directory, str(ID), 'tcp download')
    files_in_window = []
    if os.path.exists(target_directory):
        for file in os.listdir(target_directory):
            if file.endswith(".miniseed") and ID in file:
                file_path = os.path.join(target_directory, file)
                st = read(file_path)
                tr = st[0]
                file_start_time = tr.stats.starttime.datetime.replace(tzinfo=timezone.utc)
                file_end_time = tr.stats.endtime.datetime.replace(tzinfo=timezone.utc)
                if file_start_time <= end_time and file_end_time >= start_time:
                    files_in_window.append(file_path)
    return files_in_window

def read_crt_times(crt_file: str, sample_interval: float) -> List[Tuple[datetime, datetime]]:
    times = []
    with open(crt_file, 'r') as f:
        lines = f.readlines()
        header_parts = lines[0].strip().split(',')
        timestamp = datetime.strptime(header_parts[0] + header_parts[1], '%Y%m%d%H%M%S').replace(tzinfo=timezone.utc)

        for line in lines[1:]:
            parts = line.strip().split(',')
            start_index = int(parts[0])
            end_index = int(parts[1])
            start_time = timestamp + timedelta(seconds=start_index * sample_interval)
            end_time = timestamp + timedelta(seconds=end_index * sample_interval)
            times.append((start_time, end_time))
    return times

def process_stable_segments(data: np.ndarray, start_time: datetime, end_time: datetime, base_timestamp: datetime, sample_rate: float) -> float:
    start_index = int((start_time - base_timestamp).total_seconds() * sample_rate)
    end_index = int((end_time - base_timestamp).total_seconds() * sample_rate)
    
    stable_segment = data[start_index:end_index]
    mean_value, _ = remove_outliers(stable_segment)
    
    return mean_value

def copy_and_replace_crt_with_vsb(crt_file: str, vsb_data: List[Tuple[datetime, datetime, float]], output_vsb_file: str):
    shutil.copy(crt_file, output_vsb_file)
    
    with open(output_vsb_file, 'r') as f:
        lines = f.readlines()
    
    with open(output_vsb_file, 'w') as f:
        f.write(lines[0])
        for i, line in enumerate(lines[1:]):
            if i < len(vsb_data):
                start_time, end_time, avg_voltage = vsb_data[i]
                if not np.isnan(avg_voltage):
                    parts = line.strip().split(',')
                    f.write(f'{parts[0]},{parts[1]},{avg_voltage}\n')

def generate_vpm_file(vsb_file: str, crt_file: str, output_path: str, rx_id: str, timestamp: datetime) -> str:
    try:
        with open(vsb_file, 'r') as f:
            vsb_lines = f.readlines()

        with open(crt_file, 'r') as f:
            crt_lines = f.readlines()

        if len(vsb_lines) < 3:
            return None

        vpm_data = []
        for i in range(2, len(vsb_lines) - 1):
            parts_current = vsb_lines[i].strip().split(',')
            parts_next = vsb_lines[i + 1].strip().split(',')

            if len(parts_current) >= 3 and len(parts_next) >= 3:
                try:
                    voltage_current = float(parts_current[2])
                    voltage_next = float(parts_next[2])
                    voltage_diff = voltage_current - voltage_next
                    vpm_data.append(voltage_diff)
                except ValueError:
                    pass

        output_file_path = os.path.join(output_path, generate_filename(timestamp, rx_id, "vpm"))

        with open(output_file_path, 'w') as f:
            for value in vpm_data:
                sign = '+' if value >= 0 else '-'
                f.write(f"{sign}{abs(value):.3f}\n")

        return output_file_path

    except Exception as e:
        return None

def generate_vsk_file(vpm_file: str, crt_file: str, output_path: str, rx_id: str, timestamp: datetime):
    """生成叠加电压文件.vsk"""
    try:
        with open(vpm_file, 'r') as f:
            vpm_lines = f.readlines()
        
        with open(crt_file, 'r') as f:
            crt_lines = f.readlines()
        
        if len(vpm_lines) == 0 or len(crt_lines) <= 2:
            return
        
        vpm_values = [float(line.strip()) for line in vpm_lines if line.strip()]
        crt_values = [float(line.strip().split(',')[1]) for line in crt_lines[1:]]

        if len(vpm_values) > len(crt_values) - 1:
            return
        
        vsk_values = []
        for i in range(len(vpm_values)):
            try:
                crt_diff = crt_values[i + 1] - crt_values[i + 2]
                if crt_diff != 0:
                    vsk_value = vpm_values[i] / crt_diff
                    vsk_values.append(vsk_value)
            except IndexError:
                pass
        
        abs_mean_vsk = np.mean([abs(value) for value in vsk_values])

        vpm_sign = np.sign(vpm_values[0])
        crt_sign = np.sign(crt_values[1])
        final_sign = vpm_sign * crt_sign

        final_value = final_sign * abs_mean_vsk
        
        # 添加符号到结果值
        sign = '+' if final_value >= 0 else '-'
        final_value = f"{sign}{abs(final_value):.3f}"
        
        output_file_path = os.path.join(output_path, generate_filename(timestamp, rx_id, "vsk"))
        
        with open(output_file_path, 'w') as f:
            f.write(f"{final_value}\n")
    
    except Exception as e:
        pass

def process_rx_id_for_tx_id(tx_id: str, rx_id: str, combined_tx_data: np.ndarray, combined_timestamp: datetime, base_directory: str, output_directory: str, start_time: datetime, end_time: datetime, Hall_coefficient: float, sample_interval: float):
    monitor_new(combined_tx_data, combined_timestamp, Hall_coefficient, output_directory, start_time, end_time, tx_id)
    
    crt_file = os.path.join(output_directory, generate_filename(end_time, tx_id, "crt"))
    crt_times = read_crt_times(crt_file, sample_interval)
    
    sample_rate = 1 / sample_interval
    vsb_data = []

    def process_time_window(start_time, end_time):
        rx_miniseed_files = get_time_window_files(base_directory, start_time, end_time, str(rx_id))
        if rx_miniseed_files:
            rx_data_list = []
            for file in rx_miniseed_files:
                st = read(file)
                tr = st[0]
                rx_data_list.append(tr.data)
            rx_combined_data = np.concatenate(rx_data_list)
            mean_value = process_stable_segments(rx_combined_data, start_time, end_time, combined_timestamp, sample_rate)
            average_voltage = mean_value * Hall_coefficient
            return (start_time, end_time, average_voltage)
        return None

    with ThreadPoolExecutor() as executor:
        results = list(tqdm(executor.map(lambda t: process_time_window(*t), crt_times), total=len(crt_times), desc=f"Processing RX_ID {rx_id} for TX_ID {tx_id}"))
    
    vsb_data = [result for result in results if result is not None]
    
    output_vsb_filename = os.path.join(output_directory, generate_filename(end_time, rx_id, "vsb"))
    
    copy_and_replace_crt_with_vsb(crt_file, vsb_data, output_vsb_filename)
    
    vpm_file_path = generate_vpm_file(output_vsb_filename, crt_file, output_directory, rx_id, end_time)
    
    if vpm_file_path and os.path.exists(vpm_file_path):
        generate_vsk_file(vpm_file_path, crt_file, output_directory, rx_id, end_time)

def process_tx_rx(tx_id: str, rx_ids: List[str], base_directory: str, output_directory: str, stack_time_window: int, minutes_of_action: List[int], Hall_coefficient: float, sample_interval: float):
    now = datetime(2024, 6, 18, 6, 30, tzinfo=timezone.utc)  # 设置固定UTC时间

    if now.minute in minutes_of_action:
        end_time = now.replace(second=0, microsecond=0)
        start_time = end_time - timedelta(minutes=stack_time_window)

        tx_miniseed_files = get_time_window_files(base_directory, start_time, end_time, str(tx_id))
        if tx_miniseed_files:
            tx_data_list = []
            timestamps = []
            for file in tx_miniseed_files:
                st = read(file)
                tr = st[0]
                tx_data_list.append(tr.data)
                timestamps.append(tr.stats.starttime.datetime.replace(tzinfo=timezone.utc))
            
            combined_tx_data = np.concatenate(tx_data_list)
            combined_timestamp = min(timestamps)
            
            with ThreadPoolExecutor(max_workers=len(rx_ids)) as executor:
                futures = {executor.submit(process_rx_id_for_tx_id, tx_id, rx_id, combined_tx_data, combined_timestamp, base_directory, output_directory, start_time, end_time, Hall_coefficient, sample_interval): rx_id for rx_id in rx_ids}
                
                for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc=f"Processing RX_IDs for TX_ID {tx_id}"):
                    future.result()
                    print(f"Completed processing RX_ID {futures[future]} for TX_ID {tx_id}")

def main_loop(base_directory: str, output_directory: str, stack_time_window: int, minutes_of_action: List[int], tx_ids: List[int], rx_ids: List[int]):
    while True:
        now = datetime(2024, 6, 18, 6, 30, tzinfo=timezone.utc)  # 设置固定UTC时间
        print(f"Current time (UTC): {now}")  # 打印当前时间
        if now.minute in minutes_of_action:
            print(f"Minute {now.minute} is in MINUTES_OF_ACTION, processing...")
            with ThreadPoolExecutor(max_workers=len(tx_ids)) as executor:
                futures = []
                for tx_id in tx_ids:
                    Hall_coefficient, sample_interval = read_acq_parameters(base_directory, str(tx_id))
                    futures.append(executor.submit(process_tx_rx, tx_id, rx_ids, base_directory, output_directory, stack_time_window, minutes_of_action, Hall_coefficient, sample_interval))
                
                for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Processing all TX_IDs"):
                    future.result()
            time.sleep(60)  # 避免在同一分钟内多次执行
        else:
            print(f"Minute {now.minute} is not in MINUTES_OF_ACTION.")
        time.sleep(10)  # 每 10 秒检查一次时间


if __name__ == "__main__":
    json_file = r'C:\Users\xiaoyu\Desktop\AWS Lambda\pre.json'
    params = read_parameters(json_file)

    base_directory = params["INPUT_PATH"]
    output_directory = params["OUTPUT_PATH"]
    stack_time_window = params["STACK_TIME_WINDOW"] - 1
    minutes_of_action = params["MINUTES_OF_ACTION"]

    tx_ids = params["TX_ID"]
    rx_ids = params["RX_ID"]

    main_loop(base_directory, output_directory, stack_time_window, minutes_of_action, tx_ids, rx_ids)
