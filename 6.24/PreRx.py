import os
import numpy as np
from datetime import datetime
from obspy import read
from typing import List, Tuple
import json
from datetime import timedelta


sample_interval = 0.02

def read_parameters(json_file: str) -> dict:
    with open(json_file, 'r', encoding='utf-8') as f:
        parameters = json.load(f)
    return parameters


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

def write_crt_file(filename: str, peak_indices: List[Tuple[int, int]], DC_amp: np.ndarray, timestamp: datetime, sample_interval: float = 0.001):
    date_time_str = timestamp.strftime('%Y%m%d%H%M')
    seconds_str = timestamp.strftime('%S')

    with open(filename, 'w') as f:
        f.write(f'{date_time_str},{seconds_str},{sample_interval}E-3\n')
        for (start, end), amp in zip(peak_indices, DC_amp):
            f.write(f'{start}, {end}, {amp}\n')

def monitor_new(data: np.ndarray, timestamp: datetime, Hall_coefficient: float):
    window_size = 10
    peak_sort = detect_stable_segments(data, window_size)
    peak_indices, DC_amp = calculate_dc_amp(data, peak_sort, Hall_coefficient)

    output_filename = 'MonitorData.crt'
    write_crt_file(output_filename, peak_indices, DC_amp, timestamp)

    print(f'数据成功写入 {output_filename}')

def find_miniseed_file(directory: str, TX_ID: str) -> str:
    target_directory = os.path.join(directory, TX_ID, 'tcp download')
    if os.path.exists(target_directory):
        for file in os.listdir(target_directory):
            if file.endswith(".miniseed") and TX_ID in file:
                return os.path.join(target_directory, file)
    return None

def get_time_window_files(directory: str, start_time: datetime, end_time: datetime, TX_ID: str) -> List[str]:
    """
    获取时间窗口内的所有miniseed文件
    
    参数:
        directory (str): 基础目录
        start_time (datetime): 开始时间
        end_time (datetime): 结束时间
        TX_ID (str): TX_ID
    
    返回:
        List[str]: miniseed文件路径列表
    """
    target_directory = os.path.join(directory, TX_ID, 'tcp download')
    files_in_window = []
    if os.path.exists(target_directory):
        for file in os.listdir(target_directory):
            if file.endswith(".miniseed") and TX_ID in file:
                file_path = os.path.join(target_directory, file)
                st = read(file_path)
                tr = st[0]
                file_start_time = tr.stats.starttime.datetime
                file_end_time = tr.stats.endtime.datetime
                if file_start_time <= end_time and file_end_time >= start_time:
                    files_in_window.append(file_path)
    return files_in_window

def read_crt_times(crt_file: str, sample_interval: float) -> List[Tuple[datetime, datetime]]:
    """
    读取 CRT 文件中的时间点
    
    参数:
        crt_file (str): CRT 文件路径
        sample_interval (float): 采样间隔
    
    返回:
        List[Tuple[datetime, datetime]]: 每行的起止时间元组列表
    """
    times = []
    with open(crt_file, 'r') as f:
        lines = f.readlines()
        # 第一行包含了时间戳
        header_parts = lines[0].strip().split(',')
        timestamp = datetime.strptime(header_parts[0] + header_parts[1], '%Y%m%d%H%M%S')

        for line in lines[1:]:  # 跳过第一行
            parts = line.strip().split(',')
            start_index = int(parts[0])
            end_index = int(parts[1])
            start_time = timestamp + timedelta(seconds=start_index * sample_interval)
            end_time = timestamp + timedelta(seconds=end_index * sample_interval)
            times.append((start_time, end_time))
    return times


def get_time_window_files(directory: str, start_time: datetime, end_time: datetime, ID: str) -> List[str]:
    """
    获取时间窗口内的所有miniseed文件
    
    参数:
        directory (str): 基础目录
        start_time (datetime): 开始时间
        end_time (datetime): 结束时间
        ID (str): ID (TX_ID 或 RX_ID)
    
    返回:
        List[str]: miniseed文件路径列表
    """
    target_directory = os.path.join(directory, ID, 'tcp download')
    files_in_window = []
    if os.path.exists(target_directory):
        for file in os.listdir(target_directory):
            if file.endswith(".miniseed") and ID in file:
                file_path = os.path.join(target_directory, file)
                st = read(file_path)
                tr = st[0]
                file_start_time = tr.stats.starttime.datetime
                file_end_time = tr.stats.endtime.datetime
                if file_start_time <= end_time and file_end_time >= start_time:
                    files_in_window.append(file_path)
    return files_in_window

def process_stable_segments(data: np.ndarray, start_time: datetime, end_time: datetime, base_timestamp: datetime, sample_rate: float) -> float:
    """
    处理数据，截取稳定段并计算平均值
    
    参数:
        data (np.ndarray): 输入数据
        start_time (datetime): 稳定段开始时间
        end_time (datetime): 稳定段结束时间
        base_timestamp (datetime): 数据的基础时间戳
        sample_rate (float): 采样率
    
    返回:
        float: 稳定段的平均值
    """
    start_index = int((start_time - base_timestamp).total_seconds() * sample_rate)
    end_index = int((end_time - base_timestamp).total_seconds() * sample_rate)
    
    stable_segment = data[start_index:end_index]
    mean_value, _ = remove_outliers(stable_segment)
    
    return mean_value

def find_miniseed_file(directory: str, ID: str) -> str:
    """
    根据ID查找miniseed文件
    
    参数:
        directory (str): 目录路径
        ID (str): TX_ID或RX_ID
    
    返回:
        str: 找到的miniseed文件路径
    """
    target_directory = os.path.join(directory, ID, 'tcp download')
    if os.path.exists(target_directory):
        for file in os.listdir(target_directory):
            if file.endswith(".miniseed") and ID in file:
                return os.path.join(target_directory, file)
    return None

def get_time_window_files(directory: str, start_time: datetime, end_time: datetime, ID: str) -> List[str]:
    """
    获取时间窗口内的所有miniseed文件
    
    参数:
        directory (str): 基础目录
        start_time (datetime): 开始时间
        end_time (datetime): 结束时间
        ID (str): TX_ID或RX_ID
    
    返回:
        List[str]: miniseed文件路径列表
    """
    target_directory = os.path.join(directory, ID, 'tcp download')
    files_in_window = []
    if os.path.exists(target_directory):
        for file in os.listdir(target_directory):
            if file.endswith(".miniseed") and ID in file:
                file_path = os.path.join(target_directory, file)
                st = read(file_path)
                tr = st[0]
                file_start_time = tr.stats.starttime.datetime
                file_end_time = tr.stats.endtime.datetime
                if file_start_time <= end_time and file_end_time >= start_time:
                    files_in_window.append(file_path)
    return files_in_window

def write_vsb_file(filename: str, peak_indices: List[Tuple[int, int]], DC_amp: np.ndarray, timestamp: datetime, sample_interval: float = 0.001):
    date_time_str = timestamp.strftime('%Y%m%d%H%M')
    seconds_str = timestamp.strftime('%S')

    with open(filename, 'w') as f:
        f.write(f'{date_time_str},{seconds_str},{sample_interval}E-3\n')
        for (start, end), amp in zip(peak_indices, DC_amp):
            f.write(f'{start}, {end}, {amp}\n')

if __name__ == "__main__":
    # JSON 文件路径
    json_file = r'C:\Users\xiaoyu\Desktop\AWS Lambda\pre.json'
    
    # 读取参数
    params = read_parameters(json_file)
    
    TX_ID = params["TX_ID"][0]
    RX_ID = params["RX_ID"][0]
    base_directory = params["INPUT_PATH"]
    output_directory = params["OUTPUT_PATH"]
    stack_time_window = params["STACK_TIME_WINDOW"]
    minutes_of_action = params["MINUTES_OF_ACTION"]
    
    # 设置当前时间为2024年6月18号6点30分
    now = datetime(2024, 6, 18, 6, 30)
    
    # 检查当前时间是否符合条件
    if now.minute in minutes_of_action:
        end_time = now.replace(second=0, microsecond=0)
        start_time = end_time - timedelta(minutes=stack_time_window)
        
        # 获取时间窗口内的所有TX_ID的miniseed文件
        tx_miniseed_files = get_time_window_files(base_directory, start_time, end_time, str(TX_ID))
        
        if tx_miniseed_files:
            # 合并所有TX_ID的miniseed文件的数据
            tx_data_list = []
            timestamps = []
            for file in tx_miniseed_files:
                st = read(file)
                tr = st[0]
                tx_data_list.append(tr.data)
                timestamps.append(tr.stats.starttime.datetime)
            
            # 将所有数据拼接在一起
            combined_tx_data = np.concatenate(tx_data_list)
            combined_timestamp = min(timestamps)
            
            # Hall 系数示例值
            Hall_coefficient = 1.0

            # 调用主函数生成CRT文件
            monitor_new(combined_tx_data, combined_timestamp, Hall_coefficient)
            
            # 读取CRT文件中的时间点
            crt_file = os.path.join(output_directory, 'MonitorData.crt')
            sample_interval = tr.stats.delta
            crt_times = read_crt_times(crt_file, sample_interval)
            
            # 处理每个时间点范围内的RX_ID数据并生成VSB文件
            sample_rate = tr.stats.sampling_rate
            peak_indices = []
            DC_amp = []
            for start_time, end_time in crt_times:
                rx_miniseed_files = get_time_window_files(base_directory, start_time, end_time, str(RX_ID))
                
                if rx_miniseed_files:
                    rx_data_list = []
                    for file in rx_miniseed_files:
                        st = read(file)
                        tr = st[0]
                        rx_data_list.append(tr.data)
                    
                    rx_combined_data = np.concatenate(rx_data_list)
                    mean_value = process_stable_segments(rx_combined_data, start_time, end_time, combined_timestamp, sample_rate)
                    average_voltage = mean_value * Hall_coefficient
                    start_index = int((start_time - combined_timestamp).total_seconds() * sample_rate)
                    end_index = int((end_time - combined_timestamp).total_seconds() * sample_rate)
                    peak_indices.append((start_index, end_index))
                    DC_amp.append(average_voltage)
            
            # 生成输出文件名
            start_time_str = start_time.strftime('%Y%m%d.%H%M')
            end_time_str = end_time.strftime('%H%M')
            output_filename = os.path.join(output_directory, f'{start_time_str}--{end_time_str}.vsb')
            
            # 写入VSB文件
            write_vsb_file(output_filename, peak_indices, np.array(DC_amp), combined_timestamp, sample_interval)
            
            print(f'数据成功写入 {output_filename}')
        else:
            print(f"在时间窗口内未找到符合TX_ID: {TX_ID} 的miniseed文件")
    else:
        print(f"当前时间 {now.strftime('%H:%M')} 不在MINUTES_OF_ACTION之内")
