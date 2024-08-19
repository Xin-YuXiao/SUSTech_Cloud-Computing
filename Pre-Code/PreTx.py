import os
import numpy as np
from datetime import datetime, timedelta, timezone
from obspy import read
from utils import read_preprocessing_params
import glob
import csv
import json

# 使用z-score方法去除离群值的函数
def remove_outliers(data, threshold=3):
    z_scores = np.abs((data - np.mean(data)) / np.std(data))
    filtered_indices = np.where(z_scores < threshold)[0]
    mean_value = np.mean(data[filtered_indices])
    return mean_value, filtered_indices

# 检测稳态段的函数
def detect_stable_segments(data, window_size=10, threshold=0.01, min_length=10):
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

# 根据极值点计算稳定电流幅值的函数
def calculate_dc_amp(data, peak_sort, Hall_coefficient):
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

# 读取采样率和转换系数的函数
def read_acq_params(acq_csv_file_path):
    with open(acq_csv_file_path, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 4:
                Hall_coefficient = float(row[2])
                sampling_rate = float(row[3])
                return Hall_coefficient, sampling_rate
    raise ValueError("Required parameters not found in acq.csv")

# 将结果写入CRT格式文件的函数
def write_crt_file(filename, peakindices, DC_amp, timestamp, start_point, sample_interval):
    if os.path.exists(filename):
        print(f"File {filename} already exists.")
        return

    date_time_str = timestamp.strftime('%Y%m%d%H%M')

    with open(filename, 'w') as f:
        f.write(f'{date_time_str},{start_point},\n')
        f.write(f'{sample_interval}E-3\n')
        for (start, end), amp in zip(peakindices, DC_amp):
            f.write(f'{start}, {end}, {amp}\n')

    print(f'Data successfully written to {filename}')

# 主函数
def main():
    params = read_preprocessing_params(r'C:\Users\xiaoyu\Desktop\AWS Lambda\pre.json')
    if not params:
        print("Failed to read preprocessing parameters. Exiting.")
        return

    problem_files = []
    start_time = datetime.now(timezone.utc)

    for tx_id in params['TX_ID']:
        print(f"已读取 TX_ID: {tx_id}，正在处理...")
        receiver_files = glob.glob(os.path.join(params['INPUT_PATH'], f'*{tx_id}*'))
        if not receiver_files:
            print(f"Error: No files found for TX_ID {tx_id}.")
            continue

        for receiver_file in receiver_files:
            data = read(receiver_file)[0].data

            # 读取acq.csv文件路径
            acq_csv_file_path = params.get('ACQ_CSV_PATH')
            if not acq_csv_file_path or not os.path.exists(acq_csv_file_path):
                print(f"Error: ACQ CSV file {acq_csv_file_path} does not exist.")
                continue

            # 读取采样率和转换系数
            try:
                Hall_coefficient, sample_interval = read_acq_params(acq_csv_file_path)
            except ValueError as e:
                print(e)
                continue

            peak_sort = detect_stable_segments(data, window_size=10)
            peakindices, DC_amp = calculate_dc_amp(data, peak_sort, Hall_coefficient)
            timestamp = datetime.now(timezone.utc)

            # 确保输出路径存在
            os.makedirs(params['OUTPUT_PATH'], exist_ok=True)

            # 起算点可以由用户设定或根据实际情况决定，这里假设为0.0
            start_point = 0.0  # 需要根据实际情况进行调整

            crt_filename = os.path.join(params['OUTPUT_PATH'], f'{timestamp.strftime("%Y%m%d%H%M")}-{tx_id}.crt')
            write_crt_file(crt_filename, peakindices, DC_amp, timestamp, start_point, sample_interval)

            print(f"Data successfully written to {crt_filename}")
            print(f"已完成 TX_ID: {tx_id}")

            # 检查处理时间是否超过1分钟
            if (datetime.now(timezone.utc) - start_time) > timedelta(minutes=1):
                problem_files.append(crt_filename)
                with open(crt_filename, 'w') as f:
                    f.write('')  # 将文件内容设置为空
                print(f"处理超时，已将文件 {crt_filename} 设置为空。")

    if problem_files:
        print("以下文件在处理过程中出现超时问题：")
        for file in problem_files:
            print(file)

if __name__ == "__main__":
    main()
