import os
import numpy as np
from datetime import datetime, timedelta
from obspy import read
from utils import read_preprocessing_params

def remove_outliers(data, threshold=3):
    """
    使用z-score方法去除离群值。
    
    参数:
    data: np.array, 输入数据
    threshold: float, z-score阈值
    
    返回:
    mean_value: float, 过滤后的均值
    filtered_indices: np.array, 过滤后的索引
    """
    z_scores = np.abs((data - np.mean(data)) / np.std(data))
    filtered_indices = np.where(z_scores < threshold)[0]
    mean_value = np.mean(data[filtered_indices])
    return mean_value, filtered_indices

def detect_stable_segments(data, window_size=10, threshold=0.01, min_length=10):
    """
    检测稳态段。
    
    参数:
    data: np.array, 输入数据
    window_size: int, 窗口大小
    threshold: float, 阈值
    min_length: int, 最小长度
    
    返回:
    peak_sort: list, 稳态段索引
    """
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

    peak_sort = [i for i in range(1, len(data_y) - 1) if (data_y[i] - data_y[i - 1]) * (data_y[i + 1] - data_y[i]) < 0]
    peak = [peak_sort[i] for i in range(len(peak_sort) - 1) if peak_sort[i + 1] - peak_sort[i] < 20]
    peak_sort = [p for p in peak_sort if p not in peak]

    return peak_sort

def calculate_dc_amp(data, peak_sort, Hall_coefficient):
    """
    根据极值点计算稳定电流幅值。
    
    参数:
    data: np.array, 输入数据
    peak_sort: list, 极值点索引
    Hall_coefficient: float, 霍尔系数
    
    返回:
    peakindices: list, 极值点索引对
    DC_amp: np.array, 稳定电流幅值
    """
    DC_amp = []
    peakindices = []

    for i in range(len(peak_sort) - 1):
        if peak_sort[i + 1] <= len(data):
            mean_value, indices = remove_outliers(data[peak_sort[i]:peak_sort[i + 1]])
            DC_amp.append(mean_value * Hall_coefficient)
            peakindices.append((peak_sort[i] + min(indices) + 3, peak_sort[i] + max(indices) - 3))

    DC_amp = np.array(DC_amp)
    DC_amp[np.abs(DC_amp) < (np.max(DC_amp) / 2)] = 0

    return peakindices, DC_amp

def write_crt_file(filename, peakindices, DC_amp, timestamp, sample_interval=0.001):
    """
    将结果写入CRT格式文件。
    
    参数:
    filename: str, 文件名
    peakindices: list, 极值点索引对
    DC_amp: np.array, 稳定电流幅值
    timestamp: datetime, 时间戳
    sample_interval: float, 采样间隔
    """
    if os.path.exists(filename):
        print(f"File {filename} already exists.")
        return

    date_time_str = timestamp.strftime('%Y%m%d%H%M')
    seconds_str = timestamp.strftime('%S')

    with open(filename, 'w') as f:
        f.write(f'{date_time_str},{sample_interval}E-3\n')
        for (start, end), amp in zip(peakindices, DC_amp):
            f.write(f'{start}, {end}, {amp}\n')

    print(f'Data successfully written to {filename}')

def main():
    """
    主函数。
    """
    params = read_preprocessing_params(r'C:\Users\xiaoyu\Desktop\AWS Lambda\pre.json')
    if not params:
        print("Failed to read preprocessing parameters. Exiting.")
        return

    minutes_of_action = params.get('#MINUTES_OF_ACTION', [])
    current_minute = datetime.now().minute
    

    if current_minute not in minutes_of_action:
        print(f"当前时间的分钟({current_minute})不在预处理分钟列表中，程序退出。")
        return

    problem_files = []
    start_time = datetime.now()

    for tx_id in params['TX_ID']:
        for rx_id in params['RX_ID']:
            print(f"已读取 TX_ID: {tx_id}, RX_ID: {rx_id}，正在处理...")
            receiver_file = f'{tx_id}_{rx_id}.miniseed'
            receiver_path = os.path.join(params['INPUT_PATH'], tx_id, rx_id, receiver_file)
            if not os.path.exists(receiver_path):
                print(f"Error: Receiver file {receiver_path} does not exist.")
                continue

            data = read(receiver_path)[0].data
            peak_sort = detect_stable_segments(data, window_size=10)
            peakindices, DC_amp = calculate_dc_amp(data, peak_sort, 1.0)
            timestamp = datetime.now()

            crt_filename = os.path.join(params['OUTPUT_PATH'], f'{timestamp.strftime("%Y%m%d%H%M")}-{tx_id}-{rx_id}.crt')
            write_crt_file(crt_filename, peakindices, DC_amp, timestamp)

            print(f"Data successfully written to {crt_filename}")
            print(f"已完成 TX_ID: {tx_id}, RX_ID: {rx_id}")

            if (datetime.now() - start_time) > timedelta(minutes=1):
                problem_files.append(crt_filename)
                with open(crt_filename, 'w') as f:
                    f.write('')
                print(f"处理超时，已将文件 {crt_filename} 设置为空。")

    if problem_files:
        print("以下文件在处理过程中出现超时问题：")
        for file in problem_files:
            print(file)

if __name__ == "__main__":
    main()
