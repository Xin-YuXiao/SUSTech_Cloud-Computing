import os
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from scipy.stats import zscore
from obspy import read
import traceback
import glob

# 文件和目录路径
STABLE_CURRENT_FILE_PATTERN = 'C:\\Fracking\\Projects\\DemoProject\\stacked'
RECEIVER_DATA_PATH_PATTERN = 'C:\\DCCDATA\\demo\\0617\\*\\tcp download\\*.miniseed'
OUTPUT_PATH = 'C:\\Fracking\\Projects\\DemoProject'  # 替换为实际的输出路径
ACQ_CSV_FILE = 'C:\\Fracking\\Projects\\DemoProject\\device\\acq.csv'

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
            print(f"错误:获取CSV文件 {acq_csv_file} 不存在。")
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

# 主函数
def main():
    try:
        conversion_factors = read_conversion_factor(ACQ_CSV_FILE)
        start_time = datetime.now(timezone.utc)  # 使用UTC时间
        timeout_occurred = False
        all_segments = []
        all_voltages = []
        combined_rx_id = None  # 用于存储合并的 RX_ID
        first_start_sample = None  # 用于存储第一个段的起始点

        crt_files = find_crt_files(STABLE_CURRENT_FILE_PATTERN)

        for receiver_path in glob.glob(RECEIVER_DATA_PATH_PATTERN):
            receiver_file = os.path.basename(receiver_path)
            rx_id = receiver_file.split('.')[0]
            print(f"处理接收器文件：{receiver_file}")

            timestamp_str = receiver_file.split('.')[1] + receiver_file.split('.')[2][:6]
            timestamp = datetime.strptime(timestamp_str, '%Y%m%d%H%M%S').replace(tzinfo=timezone.utc)

            for crt_filename in crt_files:
                if is_processed(crt_filename, OUTPUT_PATH):
                    print(f"{crt_filename} 已处理过，跳过。")
                    continue

                stable_segments = read_stable_current_file(crt_filename)
                if not stable_segments:
                    print(f"未找到 RX_ID: {rx_id} 的稳定段。跳过。")
                    continue

                data = read_time_series(receiver_path)
                conversion_factor = conversion_factors.get(rx_id, 1.0)
                voltages = calculate_average_voltage(data, stable_segments, conversion_factor)

                all_segments.extend(stable_segments)
                all_voltages.extend(voltages)
                combined_rx_id = rx_id  # 更新 RX_ID

                if first_start_sample is None:
                    first_start_sample = stable_segments[0][0]

                print(f"已完成处理 RX_ID: {rx_id}")

        if not timeout_occurred and combined_rx_id and first_start_sample is not None:
            date_time_str = start_time.strftime('%Y%m%d%H%M%S')

            combined_vsb_filename = os.path.join(OUTPUT_PATH, f'{date_time_str}-{combined_rx_id}.vsb')
            write_combined_voltage_file(combined_vsb_filename, all_segments, all_voltages, first_start_sample, start_time)

            combined_vpm_filename = os.path.join(OUTPUT_PATH, f'{date_time_str}-{combined_rx_id}.vpm')
            write_combined_positive_negative_voltage_file(combined_vsb_filename, combined_vpm_filename)

            combined_vsk_filename = os.path.join(OUTPUT_PATH, f'{date_time_str}-{combined_rx_id}.vsk')
            write_combined_stack_voltage_file(combined_vsb_filename, combined_vsk_filename)

            print(f"数据成功写入 {combined_vsb_filename}, {combined_vpm_filename}, {combined_vsk_filename}")
        else:
            print("错误：处理过程中发生超时或未找到任何 RX_ID 或起始点未定义。")
    except Exception as e:
        print(f"主函数执行时发生错误：{e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
