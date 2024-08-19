import os
import json

# 读取预处理参数文件的函数
def read_preprocessing_params(file_path):
    with open(file_path, 'r') as file:
        params = json.load(file)
    return params

# 读取稳定电流文件的函数
def read_stable_current_file(file_path):
    if not os.path.exists(file_path):
        print(f"Error: Stable current file {file_path} does not exist.")
        return []
    stable_segments = []
    with open(file_path, 'r') as f:
        lines = f.readlines()
        for line in lines[1:]:  # 跳过第一行的时间戳
            parts = line.strip().split(', ')
            start, end, current = int(parts[0]), int(parts[1]), float(parts[2])
            stable_segments.append((start, end, current))
    return stable_segments
