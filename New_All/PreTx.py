import os
import time
import numpy as np
from datetime import datetime, timedelta, timezone
from obspy import read
from typing import List, Tuple
import json
import pandas as pd

def read_parameters(json_file: str) -> dict:
    """从JSON文件中读取参数"""
    print(f"Reading parameters from {json_file}")
    with open(json_file, 'r', encoding='utf-8') as f:
        parameters = json.load(f)
    print(f"Parameters: {parameters}")
    return parameters

def read_acq_parameters(acq_directory: str, ID: str) -> Tuple[float, float]:
    """读取设备ACQ参数"""
    acq_file_path = os.path.join(acq_directory, str(ID), 'acq.csv')
    print(f"Reading acquisition parameters from {acq_file_path}")
    if not os.path.exists(acq_file_path):
        raise FileNotFoundError(f"{acq_file_path} not found")
    
    df = pd.read_csv(acq_file_path)
    print(f"Acquisition parameters dataframe:\n{df}")
    row = df[df['ID'] == int(ID)]
    if not row.empty:
        conversion_factor = row['conversion_factor'].values[0]
        sample_interval = row['sample_interval'].values[0]
        print(f"Found parameters for ID {ID}: conversion_factor={conversion_factor}, sample_interval={sample_interval}")
        return conversion_factor, sample_interval
    else:
        raise ValueError(f"ID {ID} not found in {acq_file_path}")

def remove_outliers(data: np.ndarray, threshold: float = 3) -> Tuple[float, np.ndarray]:
    """去除数据中的离群值并计算平均值"""
    print(f"Removing outliers from data with threshold {threshold}")
    if len(data) == 0:
        return np.nan, np.array([])
    
    z_scores = np.abs((data - np.mean(data)) / np.std(data))
    filtered_indices = np.where(z_scores < threshold)[0]
    if len(filtered_indices) == 0:
        return np.nan, np.array([])
    
    mean_value = np.mean(data[filtered_indices])
    print(f"Outliers removed. Mean value: {mean_value}")
    return mean_value, filtered_indices

def detect_stable_segments(data: np.ndarray, window_size: int = 10, threshold: float = 0.01, min_length: int = 10) -> List[int]:
    """检测数据中的稳定段，返回peak值索引"""
    print(f"Detecting stable segments with window_size={window_size}, threshold={threshold}, min_length={min_length}")
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

    print(f"Detected peaks at indices: {peak_sort}")
    return peak_sort

def calculate_dc_amp(data: np.ndarray, peak_sort: List[int], conversion_factor: float) -> Tuple[List[Tuple[int, int]], np.ndarray]:
    """计算每个稳定段的直流幅度并返回时间索引和直流幅度"""
    print(f"Calculating DC amplitude for stable segments with conversion_factor={conversion_factor}")
    DC_amp = []
    peak_indices = []

    for i in range(len(peak_sort) - 1):
        if peak_sort[i + 1] <= len(data):
            mean_value, indices = remove_outliers(data[peak_sort[i]:peak_sort[i + 1]])
            if len(indices) > 0:  # 检查 indices 是否为空
                DC_amp.append(mean_value * conversion_factor)
                peak_indices.append((peak_sort[i] + min(indices) + 3, peak_sort[i] + max(indices) - 3))

    DC_amp = np.array(DC_amp)
    if DC_amp.size > 0:  # 确保 DC_amp 非空
        DC_amp[np.abs(DC_amp) < (np.max(DC_amp) / 2)] = 0

    print(f"Calculated DC amplitudes: {DC_amp}")
    return peak_indices, DC_amp

def generate_filename(timestamp: datetime, ID: str, extension: str) -> str:
    """生成文件名"""
    timestamp_str = timestamp.strftime("%Y%m%d%H%M")
    filename = f"{timestamp_str}-{ID}.{extension}"
    print(f"Generated filename: {filename}")
    return filename

def write_crt_file(filename: str, peak_indices: List[Tuple[int, int]], DC_amp: np.ndarray, timestamp: datetime, sample_interval: float):
    """将计算结果写入CRT文件"""
    print(f"Writing CRT file: {filename}")
    date_time_str = timestamp.strftime('%Y%m%d%H%M')
    seconds_str = timestamp.strftime('%S')

    with open(filename, 'w') as f:
        f.write(f'{date_time_str},{seconds_str},{sample_interval}\n')
        for (start, end), amp in zip(peak_indices, DC_amp):
            f.write(f'{start}, {end}, {amp}\n')

    print(f"Finished writing CRT file: {filename}")

def monitor_new(data: np.ndarray, timestamp: datetime, conversion_factor: float, output_directory: str, start_time: datetime, end_time: datetime, TX_ID: str, sample_interval: float):
    """监控新数据并生成CRT文件"""
    print(f"Monitoring new data for TX_ID: {TX_ID}")
    window_size = 10
    peak_sort = detect_stable_segments(data, window_size)
    peak_indices, DC_amp = calculate_dc_amp(data, peak_sort, conversion_factor)

    filename = generate_filename(end_time, TX_ID, "crt")
    output_filename = os.path.join(output_directory, filename)
    write_crt_file(output_filename, peak_indices, DC_amp, timestamp, sample_interval)  # 添加 sample_interval 参数
    if os.path.exists(output_filename):
        print(f"CRT file generated: {output_filename}")
    else:
        raise FileNotFoundError(f"CRT file not generated: {output_filename}")

def find_miniseed_file(directory: str, ID: str) -> str:
    """寻找指定ID下的miniseed文件"""
    target_directory = os.path.join(directory, ID, 'tcp download')
    print(f"Searching for miniseed file in {target_directory}")
    if os.path.exists(target_directory):
        for file in os.listdir(target_directory):
            if file.endswith(".miniseed") and ID in file:
                found_file = os.path.join(target_directory, file)
                print(f"Found miniseed file: {found_file}")
                return found_file
    print(f"No miniseed file found for ID {ID}")
    return None

def get_time_window_files(directory: str, start_time: datetime, end_time: datetime, ID: str) -> List[str]:
    """获取规定时间窗口内的miniseed文件"""
    target_directory = os.path.join(directory, str(ID), 'tcp download')
    print(f"Getting miniseed files in time window from {start_time} to {end_time} for ID {ID}")
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
    print(f"Found miniseed files: {files_in_window}")
    return files_in_window


def process_tx_id(tx_id: str, base_directory: str, output_directory: str, stack_time_window: int, minutes_of_action: List[int], conversion_factor: float, sample_interval: float):
    """处理单个TX_ID的miniseed文件并生成CRT文件"""
    print(f"Processing TX_ID {tx_id}")
    # current_time = datetime.now(timezone.utc)  # 使用当前UTC时间
    current_time = datetime.now(timezone.utc)
    # current_time = datetime(2024, 6, 18, 6, 30, tzinfo=timezone.utc)
    if current_time.minute in minutes_of_action:
        end_time = current_time.replace(second=0, microsecond=0)
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
            monitor_new(combined_tx_data, combined_timestamp, conversion_factor, output_directory, start_time, end_time, tx_id, sample_interval)  # 添加 sample_interval 参数
        else:
            print(f"No miniseed files found for TX_ID {tx_id} in the time window")
            return  # 返回而不是抛出错误


def main_loop(base_directory: str, output_directory: str, stack_time_window: int, minutes_of_action: List[int], tx_ids: List[int]):
    """主循环处理所有TX_ID"""
    processed_minutes = set()
    while True:
        # current_time = datetime.now(timezone.utc)  # 使用当前UTC时间
        current_time = datetime.now(timezone.utc)
        # current_time = datetime(2024, 6, 18, 6, 30, tzinfo=timezone.utc)
        current_minute = current_time.minute
        if current_minute in minutes_of_action and current_minute not in processed_minutes:
            for tx_id in tx_ids:
                try:
                    conversion_factor, sample_interval = read_acq_parameters(base_directory, str(tx_id))
                    process_tx_id(tx_id, base_directory, output_directory, stack_time_window, minutes_of_action, conversion_factor, sample_interval)
                except (FileNotFoundError, ValueError) as e:
                    print(f"Error processing TX_ID {tx_id}: {e}")
            processed_minutes.add(current_minute)
            time.sleep(60)  # 避免在同一分钟内多次执行
        elif current_minute not in minutes_of_action:
            processed_minutes.clear()  # 重置已处理的分钟记录
        time.sleep(20)  # 每 10 秒检查一次时间



if __name__ == "__main__":
    json_file = r'C:\Users\xiaoyu\Desktop\AWS Lambda\pre.json'
    print(f"Reading parameters from {json_file}")
    params = read_parameters(json_file)

    base_directory = params["INPUT_PATH"]
    acq_directory = params["INPUT_PATH"]
    output_directory = params["OUTPUT_PATH"]
    stack_time_window = params["STACK_TIME_WINDOW"]
    minutes_of_action = params["MINUTES_OF_ACTION"]
    tx_ids = params["TX_ID"]

    print(f"Starting main loop with parameters: base_directory={base_directory}, output_directory={output_directory}, stack_time_window={stack_time_window}, minutes_of_action={minutes_of_action}, tx_ids={tx_ids}")
    main_loop(base_directory, output_directory, stack_time_window, minutes_of_action, tx_ids)
