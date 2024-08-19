import os
import numpy as np
from datetime import datetime, timedelta, timezone
from obspy import read
from utils import read_preprocessing_params
import json
import glob
import time
import schedule
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

def remove_outliers(data, threshold=3):
    z_scores = np.abs((data - np.mean(data)) / np.std(data))
    filtered_indices = np.where(z_scores < threshold)[0]
    mean_value = np.mean(data[filtered_indices])
    return mean_value, filtered_indices

def detect_stable_segments(data, window_size=20, threshold=0.01, min_length=20):
    num_windows = len(data) - window_size + 1
    data_y = np.zeros(num_windows)

    for i in range(num_windows):
        idx = slice(i, i + window_size)
        idy = slice(i + window_size, i + 2 * window_size)
        if idy.stop <= len(data):
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

def write_crt_file(tx_id, start_time, end_time, peakindices, DC_amp, timestamp, sample_interval=0.001, output_path='output'):
    start_time_str = start_time.strftime('%Y%m%d%H%M')
    end_time_str = end_time.strftime('%Y%m%d%H%M')
    filename = f'{tx_id}-{start_time_str}-{end_time_str}.crt'
    dir_path = os.path.join(output_path, tx_id)
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, filename)

    if os.path.exists(file_path):
        print(f"File {file_path} already exists.")
        return file_path

    date_time_str = timestamp.strftime('%Y%m%d%H%M')

    with open(file_path, 'w') as f:
        f.write(f'{date_time_str},{sample_interval}E-3\n')
        for (start, end), amp in zip(peakindices, DC_amp):
            f.write(f'{start}, {end}, {amp}\n')

    print(f'Data successfully written to {file_path}')
    return file_path

def process_tx_id(tx_id, params, minutes_of_action, current_hour):
    problem_files = []
    data_accumulated = []
    peakindices_accumulated = []
    DC_amp_accumulated = []
    end_time = current_hour

    crt_files = []

    start_processing_time = datetime.now()
    print(f"Start processing TX_ID {tx_id} at {start_processing_time}")

    for action_minute in tqdm(minutes_of_action, desc=f"Processing TX_ID {tx_id}"):
        end_time += timedelta(minutes=action_minute)
        start_time = end_time - timedelta(minutes=1)

        print(f"Processing TX_ID: {tx_id} from {start_time} to {end_time}...")

        pattern = os.path.join(params['DATA_PATH'], f'{tx_id}.{start_time.strftime("%Y%m%d.%H%M")}*0000.Z.miniseed')
        matching_files = glob.glob(pattern)
        if not matching_files:
            print(f"Error: No receiver file matching {pattern}")
            continue

        receiver_path = matching_files[0]
        try:
            data = read(receiver_path)[0].data
        except Exception as e:
            print(f"Error reading file {receiver_path}: {e}")
            continue

        data_accumulated.extend(data)

        peak_sort = detect_stable_segments(np.array(data_accumulated), window_size=20)
        peakindices, DC_amp = calculate_dc_amp(np.array(data_accumulated), peak_sort, 1.0)
        peakindices_accumulated.extend(peakindices)
        DC_amp_accumulated.extend(DC_amp)

        current_timestamp = datetime.now(timezone.utc)
        crt_file = write_crt_file(tx_id, start_time, end_time, peakindices_accumulated, DC_amp_accumulated, current_timestamp, output_path=params['OUTPUT_PATH'])
        crt_files.append(crt_file)

        peakindices_accumulated = []
        DC_amp_accumulated = []
        data_accumulated = []

        if len(crt_files) >= 10:  # 每10分钟合并前10分钟的文件
            combined_peakindices = []
            combined_DC_amp = []

            for crt_file in crt_files:
                with open(crt_file, 'r') as f:
                    lines = f.readlines()[1:]  # 跳过第一行头部信息
                    for line in lines:
                        start, end, amp = line.strip().split(', ')
                        combined_peakindices.append((int(start), int(end)))
                        combined_DC_amp.append(float(amp))

            combined_peakindices = np.array(combined_peakindices)
            combined_DC_amp = np.array(combined_DC_amp)

            combined_start_time = start_time - timedelta(minutes=9)
            combined_end_time = end_time

            combined_timestamp = datetime.now(timezone.utc)
            combined_crt_file = write_crt_file(tx_id, combined_start_time, combined_end_time, combined_peakindices, combined_DC_amp, combined_timestamp, output_path=params['OUTPUT_PATH'])

            # 删除前10分钟的原始文件
            for crt_file in crt_files:
                os.remove(crt_file)
            crt_files = []

    end_processing_time = datetime.now()
    print(f"Finished processing TX_ID {tx_id} at {end_processing_time}")
    print(f"Processing time for TX_ID {tx_id}: {end_processing_time - start_processing_time}")

    return problem_files

def main():
    while True:
        try:
            params = read_preprocessing_params(r'C:\Users\xiaoyu\Desktop\AWS Lambda\pre.json')
            if not params:
                print("Failed to read preprocessing prarameters. Exiting.")
                time.sleep(60)
                continue

            print(f"Parameters: {params}")

            with open(r'C:\Users\xiaoyu\Desktop\AWS Lambda\pre.json', 'r') as f:
                json_params = json.load(f)
            minutes_of_action = json_params.get("MINUTES_OF_ACTION", [])
            print(f"Minutes of Action: {minutes_of_action}")

            current_utc_time = datetime.now(timezone.utc)
            current_hour = current_utc_time.replace(minute=0, second=0, microsecond=0)

            with ProcessPoolExecutor() as executor:
                futures = [executor.submit(process_tx_id, tx_id, params, minutes_of_action, current_hour) for tx_id in params['TX_ID']]
                problem_files = []
                for future in tqdm(futures, desc="Overall Progress"):
                    problem_files.extend(future.result())

            if problem_files:
                print("The following files had timeout issues during processing:")
                for file in problem_files:
                    print(file)

            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()
