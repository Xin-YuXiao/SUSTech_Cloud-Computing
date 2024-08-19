import numpy as np
import pandas as pd
from datetime import datetime
from obspy import read

def remove_outliers(data, threshold=3):
    """
    使用z-score方法去除离群值
    """
    z_scores = np.abs((data - np.mean(data)) / np.std(data))
    filtered_indices = np.where(z_scores < threshold)[0]
    mean_value = np.mean(data[filtered_indices])
    return mean_value, filtered_indices

def detect_stable_segments(data, window_size=10, threshold=0.01, min_length=10):
    """
    检测方波平台稳定段，返回稳定段的开始和结束索引
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

def calculate_dc_amp(data, peak_sort, Hall_coefficient):
    """
    根据极值点计算稳定电流幅值
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
    将结果写入CRT格式文件
    """
    date_time_str = timestamp.strftime('%Y%m%d%H%M')
    seconds_str = timestamp.strftime('%S')

    with open(filename, 'w') as f:
        f.write(f'{date_time_str},{seconds_str},{sample_interval}E-3\n')
        for (start, end), amp in zip(peakindices, DC_amp):
            f.write(f'{start}, {end}, {amp}\n')

def monitor_new(data, timestamp, Hall_coefficient):
    """
    主函数,检测稳定段并生成CRT文件
    """
    window_size = 10
    peak_sort = detect_stable_segments(data, window_size)
    peakindices, DC_amp = calculate_dc_amp(data, peak_sort, Hall_coefficient)

    output_filename = 'MoniterData.crt'
    write_crt_file(output_filename, peakindices, DC_amp, timestamp)

    print(f'Data successfully written to {output_filename}')

# 示例调用
if __name__ == "__main__":
    # 读取miniseed文件
    st = read(r"C:\Users\xiaoyu\Desktop\AWS Lambda\Rece-Result\600000001.20240513.090300000.Z.miniseed")
    tr = st[0]
    data = tr.data
    timestamp = tr.stats.starttime.datetime

    # Hall 系数示例值
    Hall_coefficient = 1.0

    # 调用主函数
    monitor_new(data, timestamp, Hall_coefficient)
