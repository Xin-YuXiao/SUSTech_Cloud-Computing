import os
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from scipy.stats import zscore
from obspy import read
import traceback
import glob
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

# 文件和目录路径
STABLE_CURRENT_FILE_PATTERN = r'C:\Users\xiaoyu\Desktop\AWS Lambda\output'
RECEIVER_DATA_PATH_PATTERN = r'C:\Users\xiaoyu\Desktop\data_nkd\data2'
OUTPUT_PATH = r'C:\Users\xiaoyu\Desktop\AWS Lambda\OUTPUT1'  # 替换为实际的输出路径
ACQ_CSV_FILE = r'C:\Users\xiaoyu\Desktop\data_nkd\acq.csv'

# 如果输出路径不存在，则创建它
if not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)

# 读取稳定电流文件
def read_stable_current_file(file_path):
    try:
        if not os.path.exists(file_path):
            print(f"错误：稳定电流文件 {file_path} 不存在。")
            return []
        stable_segments = []
        with open(file_path, 'r') as f:
            lines = f.readlines()
            for line in lines[1:]:  # 跳过第一行的时间戳
                parts = line.strip().split(', ')
                if len(parts) != 3:
                    print(f"警告：稳定电流文件 {file_path} 中的行格式不正确：{line}")
                    continue
                start, end, current = int(parts[0]), int(parts[1]), float(parts[2])
                stable_segments.append((start, end, current))
        return stable_segments
    except Exception as e:
        print(f"读取稳定电流文件 {file_path} 时发生错误：{e}")
        traceback.print_exc()
        return []

# 读取时间序列数据
def read_time_series(file_path):
    try:
        st = read(file_path)
        tr = st[0]
        data = tr.data
        return data
    except Exception as e:
        print(f"读取时间序列文件 {file_path} 时发生错误：{e}")
        traceback.print_exc()
        return []

# 移除异常值
def remove_outliers(data, threshold=3):
    try:
        z_scores = zscore(data)
        filtered_indices = np.where(np.abs(z_scores) < threshold)[0]
        mean_value = np.mean(data[filtered_indices])
        return mean_value, filtered_indices
    except Exception as e:
        print(f"移除异常值时发生错误：{e}")
        traceback.print_exc()
        return np.mean(data), range(len(data))

# 读取转换因子
def read_conversion_factor(acq_csv_file):
    try:
        if not os.path.exists(acq_csv_file):
            print(f"错误: 获取CSV文件 {acq_csv_file} 不存在。")
            return {}
        conversion_factors = {}
        df = pd.read_csv(acq_csv_file)
        for index, row in df.iterrows():
            tx_id, rx_id, factor = str(row['TX_ID']), str(row['RX_ID']), float(row['conversion_factor'])
            conversion_factors[rx_id] = factor
        return conversion_factors
    except Exception as e:
        print(f"读取转换因子文件 {acq_csv_file} 时发生错误：{e}")
        traceback.print_exc()
        return {}

# 计算平均电压
def calculate_average_voltage(data, segments, conversion_factor):
    try:
        voltages = []
        for start, end, _ in segments:
            if start < len(data) and end < len(data):
                segment_data = data[start:end+1]
                mean_value = np.mean(segment_data) * conversion_factor
                voltages.append(mean_value)
            else:
                voltages.append(np.nan)  # 标记为 NaN
        return voltages
    except Exception as e:
        print(f"计算平均电压时发生错误：{e}")
        traceback.print_exc()
        return []

# 合并写入电压文件
def write_combined_voltage_file(filename, all_segments, all_voltages, start_sample, timestamp, sample_interval=0.001):
    try:
        date_time_str = timestamp.strftime('%Y%m%d%H%M%S')

        with open(filename, 'w') as f:
            f.write(f'{date_time_str}, {start_sample}, {sample_interval}E-3\n')
            for (start, end, _), voltage in zip(all_segments, all_voltages):
                f.write(f'{start}, {end}, {voltage}\n')
        if not os.path.exists(filename):
            print(f"错误：未能生成文件 {filename}")
    except Exception as e:
        print(f"写入电压文件 {filename} 时发生错误：{e}")
        traceback.print_exc()

# 写入正负电压文件
def write_combined_positive_negative_voltage_file(vsb_filename, vpm_filename):
    try:
        if not os.path.exists(vsb_filename):
            print(f"错误：.vsb 文件 {vsb_filename} 不存在。")
            return

        with open(vsb_filename, 'r') as vsb_file, open(vpm_filename, 'w') as vpm_file:
            lines = vsb_file.readlines()[1:]  # 跳过文件头
            for line in lines:
                parts = line.strip().split(', ')
                if len(parts) != 3:
                    print(f"警告：.vsb 文件中的行格式不正确：{line}")
                    continue
                voltage = float(parts[2])
                pos_voltage = voltage if voltage > 0 else 0
                neg_voltage = voltage if voltage < 0 else 0
                vpm_file.write(f'{pos_voltage}\n')
                vpm_file.write(f'{neg_voltage}\n')
    except Exception as e:
        print(f"写入正负电压文件 {vpm_filename} 时发生错误：{e}")
        traceback.print_exc()

# 写入叠加电压文件
def write_combined_stack_voltage_file(vsb_filename, vsk_filename):
    try:
        if not os.path.exists(vsb_filename):
            print(f"错误：.vsb 文件 {vsb_filename} 不存在。")
            return

        with open(vsb_filename, 'r') as vsb_file:
            lines = vsb_file.readlines()[1:]  # 跳过文件头
            voltages = []
            for line in lines:
                parts = line.strip().split(', ')
                if len(parts) != 3:
                    print(f"警告：.vsb 文件中的行格式不正确：{line}")
                    continue
                voltages.append(float(parts[2]))

            valid_voltages = [v for v in voltages if not np.isnan(v)]
            abs_avg_voltage = np.mean(np.abs(valid_voltages)) if valid_voltages else 0
            first_voltage_sign = np.sign(valid_voltages[0]) if valid_voltages else 1
            final_voltage = abs_avg_voltage * first_voltage_sign

        with open(vsk_filename, 'w') as vsk_file:
            vsk_file.write(f'{final_voltage}\n')
        if not os.path.exists(vsk_filename):
            print(f"错误：未能生成文件 {vsk_filename}")
    except Exception as e:
        print(f"写入叠加电压文件 {vsk_filename} 时发生错误：{e}")
        traceback.print_exc()

# 查找 .crt 文件
def find_crt_files(search_path):
    try:
        crt_files = [f for f in os.listdir(search_path) if f.endswith('.crt')]
        return [os.path.join(search_path, crt_file) for crt_file in crt_files]
    except Exception as e:
        print(f"查找 .crt 文件时发生错误：{e}")
        traceback.print_exc()
        return []

# 检查是否已经处理过某个 .crt 文件
def is_processed(crt_filename, output_path):
    try:
        date_time_str = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        base_name = os.path.splitext(os.path.basename(crt_filename))[0]
        vsb_filename = os.path.join(output_path, f'{date_time_str}-{base_name}.vsb')
        return os.path.exists(vsb_filename)
    except Exception as e:
        print(f"检查文件 {crt_filename} 是否已处理时发生错误：{e}")
        traceback.print_exc()
        return False

# 处理每个 TX_ID 的函数
def process_tx_id(tx_id, conversion_factors):
    problem_files = []
    data_accumulated = []
    stable_segments_accumulated = []
    combined_rx_id = tx_id  # 用于存储合并的 RX_ID
    first_start_sample = None  # 用于存储第一个段的起始点
    last_processed_time = None

    pattern = os.path.join(RECEIVER_DATA_PATH_PATTERN, f'{tx_id}.*.miniseed')
    matching_files = sorted(glob.glob(pattern))

    for receiver_path in tqdm(matching_files, desc=f"Processing {tx_id}"):
        receiver_file = os.path.basename(receiver_path)
        parts = receiver_file.split('.')
        if len(parts) < 3:
            print(f"错误：文件名格式不正确 {receiver_file}")
            continue

        timestamp_str = parts[1] + parts[2][:6]
        timestamp = datetime.strptime(timestamp_str, '%Y%m%d%H%M%S').replace(tzinfo=timezone.utc)

        if last_processed_time is None:
            last_processed_time = timestamp

        stable_segments = read_stable_current_file(receiver_path.replace('miniseed', 'crt'))
        if not stable_segments:
            print(f"未找到 RX_ID: {tx_id} 的稳定段。跳过。")
            continue

        data = read_time_series(receiver_path)
        conversion_factor = conversion_factors.get(tx_id, 1.0)
        voltages = calculate_average_voltage(data, stable_segments, conversion_factor)

        stable_segments_accumulated.extend(stable_segments)
        data_accumulated.extend(data)
        if first_start_sample is None:
            first_start_sample = stable_segments[0][0]

        if (timestamp - last_processed_time).total_seconds() >= 10 * 60 or len(data_accumulated) >= 10 * 60 * 1000:  # assuming 1000 samples per second
            current_timestamp = datetime.now(timezone.utc)
            combined_vsb_filename = os.path.join(OUTPUT_PATH, f'{current_timestamp.strftime("%Y%m%d%H%M%S")}-{combined_rx_id}.vsb')
            write_combined_voltage_file(combined_vsb_filename, stable_segments_accumulated, voltages, first_start_sample, current_timestamp)

            combined_vpm_filename = os.path.join(OUTPUT_PATH, f'{current_timestamp.strftime("%Y%m%d%H%M%S")}-{combined_rx_id}.vpm')
            write_combined_positive_negative_voltage_file(combined_vsb_filename, combined_vpm_filename)

            combined_vsk_filename = os.path.join(OUTPUT_PATH, f'{current_timestamp.strftime("%Y%m%d%H%M%S")}-{combined_rx_id}.vsk')
            write_combined_stack_voltage_file(combined_vsb_filename, combined_vsk_filename)

            print(f"数据成功写入 {combined_vsb_filename}, {combined_vpm_filename}, {combined_vsk_filename}")

            stable_segments_accumulated = []
            data_accumulated = []
            first_start_sample = None
            last_processed_time = timestamp

    if stable_segments_accumulated:
        current_timestamp = datetime.now(timezone.utc)
        combined_vsb_filename = os.path.join(OUTPUT_PATH, f'{current_timestamp.strftime("%Y%m%d%H%M%S")}-{combined_rx_id}.vsb')
        write_combined_voltage_file(combined_vsb_filename, stable_segments_accumulated, voltages, first_start_sample, current_timestamp)

        combined_vpm_filename = os.path.join(OUTPUT_PATH, f'{current_timestamp.strftime("%Y%m%d%H%M%S")}-{combined_rx_id}.vpm')
        write_combined_positive_negative_voltage_file(combined_vsb_filename, combined_vpm_filename)

        combined_vsk_filename = os.path.join(OUTPUT_PATH, f'{current_timestamp.strftime("%Y%m%d%H%M%S")}-{combined_rx_id}.vsk')
        write_combined_stack_voltage_file(combined_vsb_filename, combined_vsk_filename)

        print(f"数据成功写入 {combined_vsb_filename}, {combined_vpm_filename}, {combined_vsk_filename}")

    return problem_files

# 主函数
def main():
    start_time = datetime.now()
    print(f"处理开始时间：{start_time}")

    try:
        conversion_factors = read_conversion_factor(ACQ_CSV_FILE)
        tx_ids = [os.path.splitext(f)[0] for f in os.listdir(RECEIVER_DATA_PATH_PATTERN)]
        unique_tx_ids = list(set(tx_ids))

        with ProcessPoolExecutor() as executor:
            futures = [executor.submit(process_tx_id, tx_id, conversion_factors) for tx_id in unique_tx_ids]
            for future in tqdm(futures, desc="Overall Progress"):
                future.result()

        print("所有数据处理完成。")
    except Exception as e:
        print(f"主函数执行时发生错误：{e}")
        traceback.print_exc()

    end_time = datetime.now()
    print(f"处理结束时间：{end_time}")
    print(f"总处理时间：{end_time - start_time}")

if __name__ == "__main__":
    main()
