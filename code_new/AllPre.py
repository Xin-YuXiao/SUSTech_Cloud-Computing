import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import json
from obspy import read
import time

WINDOW_SIZE = 10  # 10分钟的窗口大小
print(1)
# 配置文件路径
config_file_path = r'C:\Users\xiaoyu\Desktop\AWS Lambda\pre.json'
def load_config(file_path):
    print(f"Loading config file from {file_path}...")
    try:
        with open(file_path, 'r') as f:
            config = json.load(f)
        print("Config loaded successfully.")
        return config
    except Exception as e:
        print(f"Error loading config file: {e}")
        return {}

def read_conversion_factors(acq_file_path):
    print(f"Reading conversion factors from {acq_file_path}...")
    try:
        df = pd.read_csv(acq_file_path)
        conversion_factors = df.set_index('TX_ID')['conversion_factor'].to_dict()
        sample_intervals = df.set_index('TX_ID')['sample_interval'].to_dict()
        print("Conversion factors and sample intervals read successfully.")
        return conversion_factors, sample_intervals
    except Exception as e:
        print(f"读取转换系数文件失败: {acq_file_path}, 错误: {e}")
        return {}, {}

def wait_for_crt_file(tx_id, output_path, timeout):
    print(f"Waiting for CRT file for TX_ID {tx_id}...")
    crt_file = None
    start_time = time.time()
    while (time.time() - start_time) < timeout:
        for file in os.listdir(output_path):
            if file.endswith('.crt') and str(tx_id) in file:
                crt_file = os.path.join(output_path, file)
                break
        if crt_file:
            break
        time.sleep(1)  # 每秒检查一次
    if crt_file:
        print(f"CRT file found: {crt_file}")
    else:
        print(f"CRT file not found within timeout period for TX_ID {tx_id}")
    return crt_file

def remove_outliers(data, threshold=3):
    """使用z-score方法去除离群值。"""
    z_scores = np.abs((data - np.mean(data)) / np.std(data))
    filtered_indices = np.where(z_scores < threshold)[0]
    mean_value = np.mean(data[filtered_indices])
    return mean_value, filtered_indices

def detect_stable_segments(data, window_size=10, threshold=0.01, min_length=10):
    """检测稳态段。"""
    print("Detecting stable segments...")
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
    """根据极值点计算稳定电流幅值。"""
    print("Calculating DC amplitude...")
    peakindices = [(peak_sort[i], peak_sort[i + 1]) for i in range(0, len(peak_sort) - 1, 2)]
    DC_amp = [Hall_coefficient * np.mean(data[start:end]) for start, end in peakindices]
    print("DC amplitude calculated.")
    return peakindices, DC_amp

def read_time_series(file_path, start_time, end_time):
    """根据时间窗口读取时间序列"""
    print(f"Reading time series from {file_path} between {start_time} and {end_time}...")
    try:
        st = read(file_path)
        st.trim(start_time, end_time)
        df = pd.DataFrame(data={'timestamp': st[0].times("timestamp"), 'data': st[0].data})
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
        mask = (df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)
        print("Time series read successfully.")
        return df.loc[mask]
    except Exception as e:
        print(f"Error reading time series: {e}")
        return pd.DataFrame()

def process_waveform(time_series, conversion_factor):
    """处理发射波形，生成稳定电流数据"""
    print("Processing waveform...")
    try:
        mean_value, _ = remove_outliers(time_series['data'])
        processed_data = mean_value * conversion_factor
        print("Waveform processed successfully.")
        return pd.DataFrame({'timestamp': time_series['timestamp'], 'processed_data': processed_data})
    except Exception as e:
        print(f"Error processing waveform: {e}")
        return pd.DataFrame()

def save_crt_file(processed_data, tx_id, start_time, sample_interval, output_path):
    """保存稳定电流文件"""
    print(f"Saving CRT file for TX_ID {tx_id}...")
    try:
        timestamp_str = start_time.strftime('%Y%m%d-%H%M')
        filename = f"{tx_id}-{timestamp_str}.crt"
        output_file_path = os.path.join(output_path, filename)
        
        with open(output_file_path, 'w') as f:
            start_time_point = (processed_data['timestamp'].iloc[0] - datetime(start_time.year, start_time.month, start_time.day, tzinfo=timezone.utc)).total_seconds()
            f.write(f"{timestamp_str},{start_time_point:.1f},{sample_interval:.1f}\n")
            for idx, row in processed_data.iterrows():
                f.write(f"{row['timestamp'].timestamp()}, {row['processed_data']:.3f}\n")
        print(f"CRT file saved: {output_file_path}")
    except Exception as e:
        print(f"Error saving CRT file: {e}")

def process_tx(input_path, output_path, tx_id, stack_time_window, minutes_of_action):
    acq_file_path = os.path.join(input_path, str(tx_id), 'acq.csv')
    if os.path.exists(acq_file_path):
        conversion_factors, sample_intervals = read_conversion_factors(acq_file_path)
        conversion_factor = conversion_factors.get(tx_id, 1)
        sample_interval = sample_intervals.get(tx_id, 1)

        current_time = datetime.now(timezone.utc)
        for minute in minutes_of_action:
            target_time = current_time.replace(minute=minute, second=0, microsecond=0)
            start_time = target_time - timedelta(minutes=stack_time_window)
            
            # 构建文件名
            file_time_str = start_time.strftime('%Y%m%d.%H%M')
            file_name = f"{tx_id}.{file_time_str}000.Z.miniseed"
            file_path = os.path.join(input_path, str(tx_id), file_name)
            
            if not os.path.exists(file_path):
                print(f"文件 {file_path} 不存在")
                continue
            
            # 读取时间序列
            time_series = read_time_series(file_path, start_time, target_time)
            
            # 处理发射波形
            processed_data = process_waveform(time_series, conversion_factor)
            
            # 保存稳定电流文件
            save_crt_file(processed_data, tx_id, start_time, sample_interval, output_path)

def process_rx(input_path, output_path, tx_id, stack_time_window, minutes_of_action, timeout):
    crt_file = wait_for_crt_file(tx_id, output_path, timeout)
    if crt_file:
        acq_file_path = os.path.join(input_path, str(tx_id), 'acq.csv')
        if os.path.exists(acq_file_path):
            conversion_factors, sample_intervals = read_conversion_factors(acq_file_path)
            conversion_factor = conversion_factors.get(tx_id, 1)
            sample_interval = sample_intervals.get(tx_id, 1)

            current_time = datetime.now(timezone.utc)
            for minute in minutes_of_action:
                target_time = current_time.replace(minute=minute, second=0, microsecond=0)
                start_time = target_time - timedelta(minutes=stack_time_window)
                
                # 构建文件名
                file_time_str = start_time.strftime('%Y%m%d.%H%M')
                file_name = f"{tx_id}.{file_time_str}000.Z.miniseed"
                file_path = os.path.join(input_path, str(tx_id), file_name)
                
                if not os.path.exists(file_path):
                    print(f"文件 {file_path} 不存在")
                    continue
                
                # 读取时间序列
                time_series = read_time_series(file_path, start_time, target_time)
                
                # 处理发射波形
                processed_data = process_waveform(time_series, conversion_factor)
                
                # 保存稳定电流文件
                save_crt_file(processed_data, tx_id, start_time, sample_interval, output_path)
            
            # 生成稳定电压文件.vsb
            generate_vsb_file(crt_file, input_path, output_path, tx_id, conversion_factor)
    else:
        current_time_str = datetime.now(timezone.utc).strftime('%Y%m%d%H%M')
        empty_files = [
            f"{current_time_str}-{tx_id}.vsb",
            f"{current_time_str}-{tx_id}.vpm",
            f"{current_time_str}-{tx_id}.vsk"
        ]
        for file in empty_files:
            empty_file_path = os.path.join(output_path, file)
            with open(empty_file_path, 'w') as f:
                f.write('')  # 写入空文件
            print(f"生成空文件: {empty_file_path}")

def generate_vsb_file(crt_file, input_path, output_path, tx_id, conversion_factor):
    """生成稳定电压文件.vsb"""
    try:
        with open(crt_file, 'r') as f:
            lines = f.readlines()
        
        # 获取稳定段的起始时间和结束时间
        header = lines[0].strip().split(',')
        start_time = datetime.strptime(header[0], '%Y%m%d-%H%M').replace(tzinfo=timezone.utc)
        base_start_time = float(header[1])
        sample_interval = float(header[2])
        
        stable_segments = []
        for line in lines[1:]:
            relative_time, _ = line.strip().split(',')
            relative_time = float(relative_time)
            timestamp = start_time + timedelta(seconds=(base_start_time + relative_time))
            stable_segments.append(timestamp)
        
        stable_segments = sorted(stable_segments)
        
        # 计算稳定段的绝对起止时间
        stable_start_time = stable_segments[0]
        stable_end_time = stable_segments[-1] + timedelta(seconds=sample_interval)
        
        # 从多个电磁接收节点仪上的miniseed文件中截取对应的接收数据时间序列
        time_series = read_time_series_from_rx(input_path, tx_id, stable_start_time, stable_end_time)
        
        # 计算平均电压值
        mean_voltage = np.nan
        if not time_series.empty:
            mean_value, _ = remove_outliers(time_series['data'])
            mean_voltage = mean_value * conversion_factor
        
        # 保存稳定电压文件.vsb
        timestamp_str = start_time.strftime('%Y%m%d-%H%M')
        filename = f"{tx_id}-{timestamp_str}.vsb"
        output_file_path = os.path.join(output_path, filename)
        
        with open(output_file_path, 'w') as f:
            # 写入文件头信息
            f.write(f"{timestamp_str}, {base_start_time:.1f}, {sample_interval:.1f}\n")
            # 写入记录
            for segment in stable_segments:
                f.write(f"{segment.timestamp()}, {mean_voltage:.3f}\n")
        
        print(f"生成稳定电压文件: {output_file_path}")
        
        # 生成正负电压文件.vpm
        generate_vpm_file(output_file_path, crt_file, output_path, tx_id)
    
    except Exception as e:
        print(f"生成稳定电压文件失败: {crt_file}, 错误: {e}")

def generate_vpm_file(vsb_file, crt_file, output_path, tx_id):
    """生成正负电压文件.vpm"""
    try:
        with open(vsb_file, 'r') as f:
            vsb_lines = f.readlines()
        
        with open(crt_file, 'r') as f:
            crt_lines = f.readlines()
        
        header = crt_lines[0].strip().split(',')
        base_start_time = float(header[1])
        sample_interval = float(header[2])
        
        crt_data = []
        for line in crt_lines[1:]:
            relative_time, current = line.strip().split(',')
            crt_data.append((float(relative_time), float(current)))
        
        vsb_data = []
        for line in vsb_lines[1:]:
            timestamp, voltage = line.strip().split(',')
            vsb_data.append((float(timestamp), float(voltage)))
        
        # 对电压值进行归一化处理
        voltages = [data[1] for data in vsb_data if not np.isnan(data[1])]
        mean_voltage = np.mean(voltages)
        std_voltage = np.std(voltages)
        vsb_data = [(data[0], (data[1] - mean_voltage) / std_voltage if not np.isnan(data[1]) else np.nan) for data in vsb_data]
        
        vpm_data = []
        for i in range(len(vsb_data) - 2):
            if not np.isnan(vsb_data[i][1]) and not np.isnan(vsb_data[i+2][1]):
                diff = (vsb_data[i][1] - vsb_data[i+2][1]) / crt_data[i][1]
                vpm_data.append(diff)
        
        # 保存正负电压文件.vpm
        timestamp_str = vsb_file.split('/')[-1].split('.')[0]
        filename = f"{tx_id}-{timestamp_str}.vpm"
        output_file_path = os.path.join(output_path, filename)
        
        with open(output_file_path, 'w') as f:
            for value in vpm_data:
                sign = '+' if value >= 0 else '-'
                f.write(f"{sign}{abs(value):.3f}\n")
        
        print(f"生成正负电压文件: {output_file_path}")
        
        # 生成叠加电压文件.vsk
        generate_vsk_file(output_file_path, crt_file, output_path, tx_id)
    
    except Exception as e:
        print(f"生成正负电压文件失败: {vsb_file}, 错误: {e}")

def generate_vsk_file(vpm_file, crt_file, output_path, tx_id):
    """生成叠加电压文件.vsk"""
    try:
        with open(vpm_file, 'r') as f:
            vpm_lines = f.readlines()
        
        with open(crt_file, 'r') as f:
            crt_lines = f.readlines()
        
        vpm_values = [float(line.strip()) for line in vpm_lines if line.strip()]
        crt_first_value = float(crt_lines[1].strip().split(',')[1])
        
        abs_mean_vpm = np.mean([abs(value) for value in vpm_values])
        sign = np.sign(vpm_values[0]) * np.sign(crt_first_value)
        final_value = sign * abs_mean_vpm
        
        # 保存叠加电压文件.vsk
        timestamp_str = vpm_file.split('/')[-1].split('.')[0]
        filename = f"{tx_id}-{timestamp_str}.vsk"
        output_file_path = os.path.join(output_path, filename)
        
        with open(output_file_path, 'w') as f:
            f.write(f"{final_value:.3f}\n")
        
        print(f"生成叠加电压文件: {output_file_path}")
    
    except Exception as e:
        print(f"生成叠加电压文件失败: {vpm_file}, 错误: {e}")

def read_time_series_from_rx(input_path, tx_id, start_time, end_time):
    """读取接收数据时间序列"""
    file_time_str = start_time.strftime('%Y%m%d.%H%M')
    file_name = f"{tx_id}.{file_time_str}000.Z.miniseed"
    file_path = os.path.join(input_path, str(tx_id), file_name)
    
    if not os.path.exists(file_path):
        return pd.DataFrame(columns=['timestamp', 'data'])
    
    try:
        st = read(file_path)
        st.trim(start_time, end_time)
        df = pd.DataFrame(data={'timestamp': st[0].times("timestamp"), 'data': st[0].data})
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
        mask = (df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)
        print("RX time series read successfully.")
        return df.loc[mask]
    except Exception as e:
        print(f"Error reading RX time series: {e}")
        return pd.DataFrame()

def start_preprocessing(input_path, output_path, tx_ids, rx_ids, stack_time_window, minutes_of_action):
    timeout = (min(minutes_of_action) - 1) * 60  # 下一个预处理时刻到来前1分钟
    
    for tx_id in tx_ids:
        print(f"Starting processing for TX_ID {tx_id}...")
        process_tx(input_path, output_path, tx_id, stack_time_window, minutes_of_action)
        
    for rx_id in rx_ids:
        print(f"Starting processing for RX_ID {rx_id}...")
        process_rx(input_path, output_path, rx_id, stack_time_window, minutes_of_action, timeout)
        
    print("Processing completed for all tasks.")
