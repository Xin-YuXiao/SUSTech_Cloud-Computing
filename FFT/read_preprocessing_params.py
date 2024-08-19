# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 15:41:33 2024

@author: WDY
"""
import json

# 读取配置文件
def read_preprocessing_params(json_path):
    try:
        with open(json_path, 'r') as f:
            params = json.load(f)
        return params
    except Exception as e:
        print(f"Error reading JSON file {json_path}: {e}")
        return None