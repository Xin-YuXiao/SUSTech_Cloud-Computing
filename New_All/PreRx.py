import os
import numpy as np
from datetime import datetime, timedelta, timezone
from obspy import read
from typing import List, Tuple
import json
import pandas as pd
import time
import shutil
from PreTx import generate_filename, get_time_window_files

def read_parameters(json_file: str) -> dict:
    """从JSON文件中读取参数"""
    with open(json_file, 'r', encoding='utf-8') as f:
        parameters = json.load(f)
    print(f"Read parameters from {json_file}: {parameters}")
    return parameters

def read_acq_parameters(base_directory: str, ID: str) -> Tuple[float, float]:
    """读取设备ACQ参数"""
    acq_file_path = os.path.join(base_directory, str(ID), 'acq.csv')
    print(f"Reading acquisition parameters from {acq_file_path}")
    if not os.path.exists(acq_file_path):
        raise FileNotFoundError(f"{acq_file_path} not found")
    
    df = pd.read_csv(acq_file_path)
    row = df[df['ID'] == int(ID)]
    if not row.empty:
        conversion_factor = row['conversion_factor'].values[0]
        sample_interval = row['sample_interval'].values[0]
        print(f"Acquisition parameters for ID {ID}: conversion_factor={conversion_factor}, sample_interval={sample_interval}")
        return conversion_factor, sample_interval
    else:
        raise ValueError(f"ID {ID} not found in {acq_file_path}")

def remove_outliers(data: np.ndarray, threshold: float = 3) -> Tuple[float, np.ndarray]:
    """去除数据中的离群值并计算平均值"""
    if len(data) == 0:
        return np.nan, np.array([])

    z_scores = np.abs((data - np.mean(data)) / np.std(data))
    filtered_indices = np.where(z_scores < threshold)[0]
    if len(filtered_indices) == 0:
        return np.nan, np.array([])

    mean_value = np.mean(data[filtered_indices])
    return mean_value, filtered_indices

def read_crt_times(crt_file: str, sample_interval: float) -> List[Tuple[datetime, datetime]]:
    """读取CRT文件中的时间窗口"""
    print(f"Reading CRT times from {crt_file}")
    if not os.path.exists(crt_file) or os.path.getsize(crt_file) == 0:
        raise ValueError(f"CRT file {crt_file} is empty or not properly read.")
    
    times = []
    with open(crt_file, 'r') as f:
        lines = f.readlines()
        if not lines:
            raise ValueError(f"CRT file {crt_file} is empty or not properly read.")
        header_parts = lines[0].strip().split(',')
        timestamp = datetime.strptime(header_parts[0] + header_parts[1], '%Y%m%d%H%M%S').replace(tzinfo=timezone.utc)

        for line in lines[1:]:
            parts = line.strip().split(',')
            start_index = int(parts[0])
            end_index = int(parts[1])
            start_time = timestamp + timedelta(seconds=start_index * sample_interval)
            end_time = timestamp + timedelta(seconds=(end_index * sample_interval))
            times.append((start_time, end_time))
    return times

def process_stable_segments(conversion_factor, data: np.ndarray, start_time: datetime, end_time: datetime, base_timestamp: datetime, sample_rate: float) -> float:
    """处理稳定段数据并计算平均值"""
    start_index = int((start_time - base_timestamp).total_seconds() * sample_rate)
    end_index = int((end_time - base_timestamp).total_seconds() * sample_rate)
    
    if start_index >= len(data) or end_index > len(data) or start_index >= end_index:
        return np.nan
    
    stable_segment = data[start_index:end_index]
    mean_value, _ = remove_outliers(stable_segment)
    if np.isnan(mean_value):
        return np.nan

    mean_value *= conversion_factor  # 计算平均值并乘以转换因子
    return mean_value

def copy_and_replace_crt_with_vsb(crt_file: str, vsb_data: List[Tuple[datetime, datetime, float]], output_vsb_file: str):
    """复制CRT文件并替换数据生成VSB文件"""
    print(f"Copying CRT file to {output_vsb_file} and replacing data with VSB data")
    shutil.copy(crt_file, output_vsb_file)
    
    with open(output_vsb_file, 'r') as f:
        lines = f.readlines()
    
    if not lines:
        raise ValueError(f"CRT file {crt_file} is empty or not properly read.")
    
    with open(output_vsb_file, 'w') as f:
        f.write(lines[0])
        for i, line in enumerate(lines[1:]):
            if i < len(vsb_data):
                start_time, end_time, avg_voltage = vsb_data[i]
                if not np.isnan(avg_voltage):
                    parts = line.strip().split(',')
                    f.write(f'{parts[0]},{parts[1]},{avg_voltage}\n')

    print(f"VSB file generated: {output_vsb_file}")

def generate_vpm_file(vsb_file: str, crt_file: str, output_path: str, rx_id: str, timestamp: datetime) -> str:
    """从VSB文件生成VPM文件"""
    try:
        print(f"Generating VPM file from {vsb_file} and {crt_file}")
        with open(vsb_file, 'r') as f:
            vsb_lines = f.readlines()

        with open(crt_file, 'r') as f:
            crt_lines = f.readlines()

        if len(vsb_lines) < 3:
            raise ValueError(f"Not enough data in VSB file {vsb_file}")

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
                    vpm_data.append(np.nan)  # 标记为NaN

        output_file_path = os.path.join(output_path, generate_filename(timestamp, rx_id, "vpm"))

        with open(output_file_path, 'w') as f:
            for value in vpm_data:
                if np.isnan(value):
                    f.write("NaN\n")
                else:
                    sign = '+' if value >= 0 else '-'
                    f.write(f"{sign}{abs(value):.3f}\n")

        print(f"VPM file generated: {output_file_path}")
        return output_file_path

    except Exception as e:
        raise ValueError(f"Error generating VPM file: {e}")

def generate_vsk_file(vpm_file: str, crt_file: str, output_path: str, rx_id: str, timestamp: datetime):
    """从VPM文件生成VSK文件"""
    try:
        print(f"Generating VSK file from {vpm_file} and {crt_file}")
        with open(vpm_file, 'r') as f:
            vpm_lines = f.readlines()
        
        with open(crt_file, 'r') as f:
            crt_lines = f.readlines()
        
        if len(vpm_lines) == 0 or len(crt_lines) <= 2:
            raise ValueError(f"Not enough data in VPM file {vpm_file}")

        vpm_values = [float(line.strip()) if line.strip() != 'NaN' else np.nan for line in vpm_lines if line.strip()]
        crt_values = [float(line.strip().split(',')[2]) for line in crt_lines[1:]]

        if len(vpm_values) > len(crt_values) - 1:
            raise ValueError(f"Mismatch in data length between VPM and CRT files")
        
        vsk_values = []
        for i in range(len(vpm_values)):
            try:
                if not np.isnan(vpm_values[i]):
                    crt_diff = crt_values[i] - crt_values[i + 1]
                    if crt_diff != 0:
                        vsk_value = vpm_values[i] / crt_diff
                        vsk_values.append(vsk_value)
            except IndexError:
                pass
        
        abs_mean_vsk = np.mean(np.abs(np.array(vsk_values)))

        vpm_sign = np.sign(vpm_values[0])
        crt_sign = np.sign(crt_values[0])
        final_sign = vpm_sign * crt_sign

        final_value = final_sign * abs_mean_vsk
        
        sign = '+' if final_value >= 0 else '-'
        final_value = f"{sign}{abs(final_value):.3f}"
        
        output_file_path = os.path.join(output_path, generate_filename(timestamp, rx_id, "vsk"))
        
        with open(output_file_path, 'w') as f:
            f.write(f"{final_value}\n")

        print(f"VSK file generated: {output_file_path}")

    except Exception as e:
        raise ValueError(f"Error generating VSK file: {e}")

def process_rx_id_for_tx_id(tx_id: str, rx_id: str, base_directory: str, output_directory: str, sample_interval: float, conversion_factor: float, crt_file: str):
    """处理单个RX_ID的数据并生成相应的VSB、VPM和VSK文件"""
    try:
        print(f"Processing RX_ID {rx_id} for TX_ID {tx_id}")
        crt_times = read_crt_times(crt_file, sample_interval)
        
        sample_rate = 1 / sample_interval
        vsb_data = []

        crt_start_times = [start for start, end in crt_times]
        crt_end_times = [end for start, end in crt_times]
        
        rx_miniseed_files = get_time_window_files(base_directory, min(crt_start_times), max(crt_end_times), str(rx_id))
        if rx_miniseed_files:
            print(f"Found miniseed files for RX_ID {rx_id}: {rx_miniseed_files}")
            rx_data_list = []
            for file in rx_miniseed_files:
                st = read(file)
                tr = st[0]
                rx_data_list.append(tr.data)
            
            if rx_data_list:
                rx_combined_data = np.concatenate(rx_data_list)
                combined_timestamp = min(crt_start_times)
                for start_time, end_time in crt_times:
                    mean_value = process_stable_segments(conversion_factor, rx_combined_data, start_time, end_time, combined_timestamp, sample_rate)
                    average_voltage = mean_value
                    vsb_data.append((start_time, end_time, average_voltage))

                output_vsb_filename = os.path.join(output_directory, generate_filename(combined_timestamp, rx_id, "vsb"))
                copy_and_replace_crt_with_vsb(crt_file, vsb_data, output_vsb_filename)
                
                if os.path.exists(output_vsb_filename) and os.path.getsize(output_vsb_filename) > 0:
                    vpm_file_path = generate_vpm_file(output_vsb_filename, crt_file, output_directory, rx_id, combined_timestamp)
                    
                    if vpm_file_path and os.path.exists(vpm_file_path):
                        generate_vsk_file(vpm_file_path, crt_file, output_directory, rx_id, combined_timestamp)
                else:
                    print(f"Generated VSB file for RX_ID {rx_id} is empty, skipping further processing.")
            else:
                print(f"No valid data found in miniseed files for RX_ID {rx_id}")
        else:
            print(f"No miniseed files found for RX_ID {rx_id} in the specified time window")

    except Exception as e:
        print(f"Error processing RX_ID {rx_id} for TX_ID {tx_id}: {e}")

def get_latest_crt_file(directory: str, tx_id: str) -> str:
    """获取最新且内容长度最长的CRT文件"""
    print(f"Getting latest CRT file for TX_ID {tx_id} from {directory}")
    files = [f for f in os.listdir(directory) if f.endswith(".crt") and f"{tx_id}" in f]
    if not files:
        print(f"No CRT files found for TX_ID {tx_id} in {directory}")
        return None

    max_length = 0
    latest_file = None
    for file in files:
        file_path = os.path.join(directory, file)
        with open(file_path, 'r') as f:
            lines = f.readlines()
            length = len(lines)
            if length > max_length:
                max_length = length
                latest_file = file_path
            elif length == max_length:
                if np.random.rand() > 0.5:
                    latest_file = file_path
    print(f"Latest CRT file for TX_ID {tx_id}: {latest_file}")
    return latest_file


def wait_for_crt_file(tx_id: int, output_directory: str, timeout: int = 300) -> str:
    """等待带有指定TX_ID的CRT文件生成"""
    crt_filename = generate_filename(datetime.now(timezone.utc), tx_id, "crt")
    crt_filepath = os.path.join(output_directory, crt_filename)
    
    wait_time = 0
    while not os.path.exists(crt_filepath) and wait_time < timeout:
        print(f"Waiting for CRT file {crt_filepath} for TX_ID {tx_id} at {datetime.now(timezone.utc)} UTC")
        time.sleep(5)
        wait_time += 5

    if os.path.exists(crt_filepath) and os.path.getsize(crt_filepath) > 0:
        print(f"CRT file {crt_filepath} for TX_ID {tx_id} found at {datetime.now(timezone.utc)} UTC.")
        return crt_filepath
    else:
        print(f"CRT file for TX_ID {tx_id} not found or empty within {timeout} seconds at {datetime.now(timezone.utc)} UTC.")
        return None

def process_tx_rx(tx_id: str, rx_ids: List[str], base_directory: str, output_directory: str, sample_interval: float, conversion_factor: float, crt_file: str):
    """处理指定TX_ID和RX_ID列表的数据"""
    print(f"Processing TX_ID {tx_id} with RX_IDs {rx_ids}")
    latest_crt_file = get_latest_crt_file(output_directory, tx_id)
    if latest_crt_file:
        for rx_id in rx_ids:
            process_rx_id_for_tx_id(tx_id, rx_id, base_directory, output_directory, sample_interval, conversion_factor, crt_file)
    else:
        print(f"No valid CRT file found for TX_ID {tx_id}")
      
def main_loop(base_directory: str, output_directory: str, minutes_of_action: List[int], tx_ids: List[int], rx_ids: List[int]):
    """主循环，定期检查并处理数据"""
    print("Starting main loop")
    while True:
        # 使用固定时间
        current_time = datetime.now(timezone.utc)
        # current_time = datetime(2024, 6, 18, 6, 30, tzinfo=timezone.utc)
        current_minute = current_time.minute
        if current_minute in minutes_of_action:
            print(f"Current time {current_time}: Checking TX_IDs {tx_ids}")
            for tx_id in tx_ids:
                try:
                    crt_filepath = wait_for_crt_file(tx_id, output_directory)
                    if crt_filepath:
                        print(f"Found CRT file for TX_ID {tx_id}: {crt_filepath}")
                        conversion_factor, sample_interval = read_acq_parameters(base_directory, str(tx_id))
                        process_tx_rx(tx_id, rx_ids, base_directory, output_directory, sample_interval, conversion_factor, crt_filepath)
                    else:
                        print(f"Skipping TX_ID {tx_id} as no valid CRT file found.")
                except Exception as e:
                    print(f"Error processing TX_ID {tx_id}: {e}")
            time.sleep(60)
        else:
            print(f"Current time {current_time}: Not in action minutes {minutes_of_action}")
            time.sleep(30)


if __name__ == "__main__":
    json_file = r'C:\Users\xiaoyu\Desktop\AWS Lambda\pre.json'
    params = read_parameters(json_file)

    base_directory = params["INPUT_PATH"]
    output_directory = params["OUTPUT_PATH"]
    minutes_of_action = params["MINUTES_OF_ACTION"]

    tx_ids = params["TX_ID"]
    rx_ids = params["RX_ID"]

    print(f"Starting main loop with parameters: base_directory={base_directory}, output_directory={output_directory}, minutes_of_action={minutes_of_action}, tx_ids={tx_ids}, rx_ids={rx_ids}")
    main_loop(base_directory, output_directory, minutes_of_action, tx_ids, rx_ids)
