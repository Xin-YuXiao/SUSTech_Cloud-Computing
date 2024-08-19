# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 15:39:56 2024

@author: WDY
"""

# 读取时间序列文件的函数
from obspy import read
def read_time_series(file_path):
    st = read(file_path)
    tr = st[0]
    data = tr.data
    return data