# PreTx.py
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from obspy import read

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

def remove_outliers(data, threshold=3):
    """使用z-score方法去除离群值。"""
    z_scores = np.abs((data - np.mean(data)) / np.std(data))
    filtered_indices = np.where(z_scores < threshold)[0]
    mean_value = np.mean(data[filtered_indices])
    return mean_value, filtered_indices

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
            for tx_id, row in processed_data.iterrows():
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
