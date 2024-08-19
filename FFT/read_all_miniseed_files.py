# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 15:42:23 2024

@author: WDY
"""
import os
from read_time_series import read_time_series
# 读取文件夹中所有miniseed文件的函数
def read_all_miniseed_files(directory):
    miniseed_data = []
    for root, dirs, files in os.walk(directory):
        for filename in files:
            if filename.endswith(".miniseed"):  # 根据实际文件扩展名调整
                file_path = os.path.join(root, filename)
                try:
                    data = read_time_series(file_path)
                    miniseed_data.append((filename, data))
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")
    return miniseed_data