import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from obspy import read
from utils import read_preprocessing_params
import json
import glob
import time
import schedule

# 使用z-score方法去除离群值的函数
def remove_outliers(data, threshold=3):
    z_scores = np.abs((data - np.mean(data)) / np.std(data))
    filtered_indices = np.where(np.abs(z_scores) < threshold)[0]
    mean_value = np.mean(data[filtered_indices])
    return mean_value, filtered_indices

# 检测稳态段的函数
def detect_stable_segments(data, window_size=20, threshold=0.01, min_length=20):
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
        if peak_sort[i + 1] - peak_sort[i] < min_length:
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

    if DC_amp.size == 0:
        return peakindices, DC_amp

    DC_amp[np.abs(DC_amp) < (np.max(DC_amp) / 2)] = 0

    return peakindices, DC_amp

# 将结果写入CRT格式文件的函数
def write_crt_file(filename, peakindices, DC_amp, timestamp, sample_interval=0.001):
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

# 合并CRT文件的函数
def merge_crt_files():
    current_time = datetime.now(timezone.utc)
    start_time = current_time - timedelta(minutes=10)
    end_time = current_time

    start_time_str = start_time.strftime('%Y%m%d%H%M')
    end_time_str = end_time.strftime('%Y%m%d%H%M')

    output_directory = 'C:\\Fracking\\Projects\\DemoProject\\stacked'
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
        
    output_filename = os.path.join(output_directory, f'merged_{start_time_str}_{end_time_str}.crt')
    crt_files = glob.glob(f'{start_time_str}_*.crt')

    with open(output_filename, 'w') as outfile:
        for fname in crt_files:
            with open(fname) as infile:
                outfile.write(infile.read())
            os.remove(fname)  # 合并后删除原文件

    print(f"Files from {start_time_str} to {end_time_str} have been merged into {output_filename}")

# 主函数
def main():
    # 每5分钟执行一次合并操作
    schedule.every(5).minutes.do(merge_crt_files)

    while True:
        # 读取预处理参数
        params = read_preprocessing_params('C:\\Fracking\\Projects\\DemoProject\\json\\pre.json')
        if not params:
            print("Failed to read preprocessing parameters. Exiting.")
            return

        # 从JSON文件中读取时间信息
        with open('C:\\Fracking\\Projects\\DemoProject\\json\\pre.json', 'r') as f:
            json_params = json.load(f)
        minutes_of_action = json_params.get("MINUTES_OF_ACTION", [])

        problem_files = []

        # 获取当前UTC时间
        current_utc_time = datetime.now(timezone.utc)
        current_hour = current_utc_time.replace(
            #day=17,hour=9,
            minute=0, second=0, microsecond=0)

        # 默认过去十分钟的数据处理
        default_start_time = current_utc_time - timedelta(minutes=10)
        default_end_time = current_utc_time

        # 生成时间范围内的时间戳
        time_windows = [(current_hour + timedelta(minutes=minutes_of_action[i]), current_hour + timedelta(minutes=minutes_of_action[i + 1])) for i in range(len(minutes_of_action) - 1)]
        if current_utc_time.minute not in minutes_of_action:
            time_windows.append((default_start_time, default_end_time))

        for tx_id in params['TX_ID']:
            print(f"已读取 TX_ID: {tx_id}，正在处理...")
            for start_time, end_time in time_windows:
                for timestamp in pd.date_range(start_time, end_time, freq='T', tz='UTC'):
                    pattern = os.path.join('C:\\DCCDATA\\demo\\0617\\*\\tcp download', f'{tx_id}.{timestamp.strftime("%Y%m%d.%H%M")}*0000.Z.miniseed')
                    matching_files = glob.glob(pattern)
                    if not matching_files:
                        print(f"Error: No receiver file matching {pattern}")
                        continue

                    receiver_path = matching_files[0]
                    data = read(receiver_path)[0].data
                    peak_sort = detect_stable_segments(data, window_size=20)
                    peakindices, DC_amp = calculate_dc_amp(data, peak_sort, 1.0)
                    current_timestamp = datetime.now(timezone.utc)

                    # 使用生成时刻的UTC时间和TX_ID号生成CRT文件名
                    crt_filename = os.path.join(params['OUTPUT_PATH'], f'{current_timestamp.strftime("%Y%m%d%H%M")}-{tx_id}.crt')
                    write_crt_file(crt_filename, peakindices, DC_amp, current_timestamp)

                    print(f"Data successfully written to {crt_filename}")
                    print(f"已完成 TX_ID: {tx_id}")

                    # 检查处理时间是否超过1分钟
                    if (datetime.now(timezone.utc) - current_utc_time) > timedelta(minutes=100):
                        problem_files.append(crt_filename)
                        with open(crt_filename, 'w') as f:
                            f.write('')  # 将文件内容设置为空
                        print(f"处理超时，已将文件 {crt_filename} 设置为空。")

        if problem_files:
            print("以下文件在处理过程中出现超时问题：")
            for file in problem_files:
                print(file)

        # 检查并执行定时任务
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
