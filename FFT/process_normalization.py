# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 16:10:38 2024

@author: WDY
"""
import os
import pandas as pd
import csv
import warnings

def extract_date_time_segment(filename):
    parts = filename.split('.')
    if len(parts) >= 3:
        return f"{parts[1]}.{parts[2]}"
    return None

def write_normalization_results_to_csv(filename, results_df):
    results_df.to_csv(filename, index=False, float_format='%.6f')


def write_normalization_results_to_txt(filename, results):
    with open(filename, 'w') as f:
        # 写入表头
        f.write("{:<12} {:<20} {:<20}\n".format("Frequency", "Amplitude_Result", "Phase_Result"))
        
        # 写入数据
        for result in results:
            f.write("{:<12.6f} {:<20.6f} {:<20.6f}\n".format(result[0], result[1], result[2]))
            
def process_csv_files(a_dir, b_dir, output_dir, amplitude_coefficient):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    warnings.filterwarnings('ignore')
    for a_filename in os.listdir(a_dir):
        if a_filename.endswith(".csv"):
            a_date_time_segment = extract_date_time_segment(a_filename)
            if a_date_time_segment is None:
                print(f"Invalid filename format: {a_filename}")
                continue

            # 查找b_dir中具有相同日期和时间段字符的文件
            b_filename = None
            for file in os.listdir(b_dir):
                if file.endswith(".csv") and extract_date_time_segment(file) == a_date_time_segment:
                    b_filename = file
                    break

            if b_filename is None:
                print(f"Corresponding file not found in b directory for date and time segment: {a_date_time_segment}")
                continue
            
            a_filepath = os.path.join(a_dir, a_filename)
            b_filepath = os.path.join(b_dir, b_filename)
            
            # 读取文件并指定列名，防止错位
            a_df = pd.read_csv(a_filepath,index_col=False)
            
            b_df = pd.read_csv(b_filepath,index_col=False)
            
            if 'Amplitude' not in a_df.columns or 'Amplitude' not in b_df.columns:
                print(f"'Amplitude' column not found in one of the files: {a_filename}")
                continue
                
            if 'Phase' not in a_df.columns or 'Phase' not in b_df.columns:
                print(f"'Phase' column not found in one of the files: {a_filename}")
                continue
            
            # Amplitude processing
            a_amplitude = a_df['Amplitude'] * amplitude_coefficient
            b_amplitude = b_df['Amplitude']
            amplitude_result = b_amplitude/a_amplitude
            
            # Phase processing
            a_phase = a_df['Phase']
            b_phase = b_df['Phase']
            phase_result = a_phase - b_phase
            
            result_df = pd.DataFrame({
                'Frequency': a_df['Frequency'],  # Assuming both have same 'Frequency' column
                'Amplitude_Result': amplitude_result,
                'Phase_Result': phase_result
            })
            
            output_filepath = os.path.join(output_dir, f"processed_{a_filename}")
            write_normalization_results_to_csv(output_filepath, result_df)
            
            # Writing to TXT file
            results = list(zip(a_df['Frequency'], amplitude_result, phase_result))
            output_filepath_txt = os.path.join(output_dir, f"processed_{a_filename.replace('.csv', '.txt')}")
            write_normalization_results_to_txt(output_filepath_txt, results)
           # result_df.to_csv(output_filepath, index=False)
            #print(f"Processed file saved to {output_filepath}")