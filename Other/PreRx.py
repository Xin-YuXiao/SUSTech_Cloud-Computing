import os
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from scipy.stats import zscore
from obspy import read

# 定义路径
STABLE_CURRENT_FILE_PATTERN = 'C:\\Users\\xiaoyu\\Desktop\\AWS Lambda\\output'
RECEIVER_DATA_PATH = 'C:\\Users\\xiaoyu\\Desktop\\data_nkd\\data2'  
OUTPUT_PATH = 'C:\\Users\\xiaoyu\\Desktop\\AWS Lambda\\output'  # 替换为实际的输出路径
ACQ_CSV_FILE = 'C:\\Users\\xiaoyu\\Desktop\\data_nkd\\acq.csv'

# 创建输出文件夹（如果不存在）
if not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)

# 读取稳定电流文件的函数
def read_stable_current_file(file_path):
    if not os.path.exists(file_path):
        print(f"Error: Stable current file {file_path} does not exist.")
        return []
    stable_segments = []
    with open(file_path, 'r') as f:
        lines = f.readlines()
        for line in lines[1:]:  # 跳过第一行的时间戳
            parts = line.strip().split(', ')
            start, end, current = int(parts[0]), int(parts[1]), float(parts[2])
            stable_segments.append((start, end, current))
    return stable_segments

# 读取时间序列文件的函数
def read_time_series(file_path):
    st = read(file_path)
    tr = st[0]
    data = tr.data
    return data

# 去除离群值的函数
def remove_outliers(data, threshold=3):
    z_scores = zscore(data)
    filtered_indices = np.where(np.abs(z_scores) < threshold)[0]
    mean_value = np.mean(data[filtered_indices])
    return mean_value, filtered_indices

# 读取转换系数
def read_conversion_factor(acq_csv_file):
    if not os.path.exists(acq_csv_file):
        print(f"Error: Acquisition CSV file {acq_csv_file} does not exist.")
        return {}
    conversion_factors = {}
    df = pd.read_csv(acq_csv_file)
    for index, row in df.iterrows():
        tx_id, rx_id, factor = str(row['TX_ID']), str(row['RX_ID']), float(row['conversion_factor'])
        conversion_factors[rx_id] = factor
    return conversion_factors

# 计算平均电压值的函数
def calculate_average_voltage(data, segments, conversion_factor):
    voltages = []
    for start, end, _ in segments:
        if start < len(data) and end < len(data):
            segment_data = data[start:end+1]
            mean_value = np.mean(segment_data) * conversion_factor
            voltages.append(mean_value)
        else:
            voltages.append(np.nan)  # 标记为 NaN
    return voltages

# 写入稳定电压文件的函数
def write_voltage_file(filename, segments, voltages, timestamp, sample_interval=0.001):
    date_time_str = timestamp.strftime('%Y%m%d%H%M')
    start_sample = segments[0][0] if segments else 0

    with open(filename, 'w') as f:
        f.write(f'{date_time_str}, {sample_interval}E-3\n')
        for (start, end, _), voltage in zip(segments, voltages):
            f.write(f'{start}, {end}, {voltage}\n')

# 写入正负电压文件的函数
def write_positive_negative_voltage_file(filename, segments, voltages):
    with open(filename, 'w') as f:
        for (start, end, _), voltage in zip(segments, voltages):
            pos_voltage = voltage if voltage > 0 else 0
            neg_voltage = voltage if voltage < 0 else 0
            f.write(f'{start}, {end}, {pos_voltage}, {neg_voltage}\n')

# 写入叠加电压文件的函数
def write_stack_voltage_file(filename, segments, voltages):
    with open(filename, 'w') as f:
        cumulative_voltage = 0
        for (start, end, _), voltage in zip(segments, voltages):
            cumulative_voltage += voltage
            f.write(f'{start}, {end}, {cumulative_voltage}\n')

# 查找包含 RX_ID 的 .crt 文件
def find_crt_file(rx_id, search_path):
    for file in os.listdir(search_path):
        if file.endswith('.crt') and rx_id in file:
            return os.path.join(search_path, file)
    return None

# 主函数
def main():
    conversion_factors = read_conversion_factor(ACQ_CSV_FILE)
    start_time = datetime.now()
    timeout_occurred = False

    for receiver_file in os.listdir(RECEIVER_DATA_PATH):
        if receiver_file.endswith('.miniseed'):
            rx_id = receiver_file.split('.')[0]
            receiver_path = os.path.join(RECEIVER_DATA_PATH, receiver_file)
            
            # 查找包含 RX_ID 的 .crt 文件
            crt_filename = None
            while not crt_filename:
                crt_filename = find_crt_file(rx_id, STABLE_CURRENT_FILE_PATTERN)
                if not crt_filename:
                    if (datetime.now() - start_time) > timedelta(minutes=1):
                        timeout_occurred = True
                        print(f"Timeout waiting for .crt file for RX_ID: {rx_id}")
                        break
                    print(f"Waiting for .crt file for RX_ID: {rx_id} to appear...")
                    time.sleep(10)

            if timeout_occurred:
                break

            stable_segments = read_stable_current_file(crt_filename)
            if not stable_segments:
                print(f"No stable segments found for RX_ID: {rx_id}. Skipping.")
                continue

            data = read_time_series(receiver_path)
            conversion_factor = conversion_factors.get(rx_id, 1.0)
            voltages = calculate_average_voltage(data, stable_segments, conversion_factor)
            
            timestamp = datetime.now()
            vsb_filename = os.path.join(OUTPUT_PATH, f'{timestamp.strftime("%Y%m%d%H%M")}-{rx_id}.vsb')
            vpm_filename = os.path.join(OUTPUT_PATH, f'{timestamp.strftime("%Y%m%d%H%M")}-{rx_id}.vpm')
            vsk_filename = os.path.join(OUTPUT_PATH, f'{timestamp.strftime("%Y%m%d%H%M")}-{rx_id}.vsk')

            write_voltage_file(vsb_filename, stable_segments, voltages, timestamp)
            write_positive_negative_voltage_file(vpm_filename, stable_segments, voltages)
            write_stack_voltage_file(vsk_filename, stable_segments, voltages)

            print(f'Data successfully written to {vsb_filename}, {vpm_filename}, {vsk_filename}')

    if timeout_occurred:
        print("Error: Timeout occurred during processing.")
    else:
        print("Success: All files processed successfully.")

if __name__ == "__main__":
    main()
