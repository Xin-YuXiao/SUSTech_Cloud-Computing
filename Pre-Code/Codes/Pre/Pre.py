import os
import json
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from obspy import read
from utils import read_preprocessing_params, read_stable_current_file

# 从PreTx_new.py中提取的函数
def remove_outliers(data, threshold=3):
    z_scores = np.abs((data - np.mean(data)) / np.std(data))
    filtered_indices = np.where(np.abs(z_scores) < threshold)[0]
    mean_value = np.mean(data[filtered_indices])
    return mean_value, filtered_indices

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
    for i in range(1, len(data_y)-1):
        if data_y[i-1] == 0 and data_y[i] != 0 and data_y[i+1] != 0:
            peak_sort.append((i, data_y[i]))

    peak_sort.sort(key=lambda x: abs(x[1]), reverse=True)

    stable_segments = []
    for peak in peak_sort:
        start = peak[0]
        end = start + window_size
        if all(data[start:end] < threshold):
            stable_segments.append((start, end))

    stable_segments = [seg for seg in stable_segments if seg[1] - seg[0] >= min_length]
    return stable_segments

# 从PreRx.py中提取的函数
def preprocess_data(params):
    input_path = params['INPUT_PATH']
    rx_id = params.get('RX_ID', None)
    if rx_id:
        print(f"RX_ID identified: {rx_id}. Using PreTx.")
        # 这里应该调用PreTx的具体实现函数
    else:
        print("No RX_ID identified. Using PreRx.")
        # 这里应该调用PreRx的具体实现函数

# 从Pre.py中提取的主逻辑
PARAMS_FILE = r'C:\\Fracking\\Projects\\DemoProject\\json\\pre.json'
OUTPUT_PATH = r'C:\\Fracking\\Projects\\DemoProject\\stacked'  # 替换为实际的输出路径

if not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)

def main():
    params = read_preprocessing_params(PARAMS_FILE)
    required_keys = ['STACK_TIME_WINDOW', 'MINUTES_OF_ACTION', 'INPUT_PATH', 'OUTPUT_PATH', 'TX_ID', 'RX_ID']
    for key in required_keys:
        if key not in params:
            raise KeyError(f'Missing required parameter: {key}')
    print("Parameters loaded successfully.")
    preprocess_data(params)

    # 每5分钟执行一次
    time.sleep(300)

if __name__ == "__main__":
    main()
