# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 15:47:30 2024

@author: WDY
"""
import os
from read_all_miniseed_files import read_all_miniseed_files
from read_preprocessing_params import read_preprocessing_params
from perform_fft import perform_fft
from write_results import write_results_to_csv
from write_results import write_results_to_txt
from process_normalization import process_csv_files


# # 主函数
# def main():
# 从JSON文件读取配置信息
#params = read_preprocessing_params('pre.json') 
params = read_preprocessing_params('C:/Users/WDY/Desktop/document/smartsolo/fftprocessing/fft.json')  # JSON路径
if not params:
    print("Failed to read preprocessing parameters. Exiting.")
    #return

# 保存所有Tx处理数据到CSV
directory_Tx = params["INPUT_PATH_Tx"]
output_directory_Tx = params["OUTPUT_PATH_Tx"]
frequencies = [1.0, 3.0, 5.0, 10.0]  # 需要提取幅值的频率,根据需要调整
sampling_rate = 500.0  # 采样率, 根据实际情况调整

miniseed_data_Tx = read_all_miniseed_files(directory_Tx)

all_results_Tx = []
for filename, data in miniseed_data_Tx:
    results_Tx = perform_fft(data, sampling_rate, frequencies)
    all_results_Tx.extend(results_Tx)
    output_csv_filename = os.path.join(output_directory_Tx, f"{os.path.splitext(filename)[0]}.csv")
    output_txt_filename = os.path.join(output_directory_Tx, f"{os.path.splitext(filename)[0]}.txt")
    write_results_to_csv(output_csv_filename, results_Tx)
    write_results_to_txt(output_txt_filename, results_Tx)
    print(f"CSV results saved to {output_csv_filename}")
    
# 保存所有Rx处理数据到CSV
directory_Rx = params["INPUT_PATH_Rx"]
output_directory_Rx = params["OUTPUT_PATH_Rx"]
miniseed_data_Rx = read_all_miniseed_files(directory_Rx)
all_results_Rx = []
for filename, data in miniseed_data_Rx:
    results_Rx = perform_fft(data, sampling_rate, frequencies)
    all_results_Rx.extend(results_Rx)
    output_csv_filename = os.path.join(output_directory_Rx, f"{os.path.splitext(filename)[0]}.csv")
    output_txt_filename = os.path.join(output_directory_Rx, f"{os.path.splitext(filename)[0]}.txt")
    write_results_to_csv(output_csv_filename, results_Rx)
    write_results_to_txt(output_txt_filename, results_Rx)
    print(f"results saved to {output_csv_filename} and {output_txt_filename}")

# 对CSV中数据进行归一化处理
output_normalization = params['OUTPUT_PATH_Normalization']
process_csv_files(directory_Tx, directory_Rx, output_normalization, amplitude_coefficient=1)

# 指定要保存的文件序号并保存为TXT
# file_index = 1  # 根据需要调整
# if file_index < len(miniseed_data):
#     filename, data = miniseed_data[file_index]
#     results = perform_fft(data, sampling_rate, frequencies)
#     output_txt_filename = os.path.join(output_directory, f"{os.path.splitext(filename)[0]}.txt")
#     write_results_to_txt(output_txt_filename, results)
#     print(f"TXT results saved to {output_txt_filename}")
# else:
#     print(f"Invalid file index: {file_index}")

# if __name__ == "__main__":
#     main()