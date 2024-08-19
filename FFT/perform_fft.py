# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 15:43:53 2024

@author: WDY
"""

import numpy as np
from scipy.fft import fft, fftfreq

# 对数据进行傅里叶变换并提取指定频率及其附近频率的结果
def perform_fft(data, sampling_rate, frequencies, bandwidth=0.1):
    n = len(data)
    T = 1.0 / sampling_rate
    yf = fft(data)
    xf = fftfreq(n, T)[:n//2]
    
    results = []
    for freq in frequencies:
        idx = (np.abs(xf - freq)).argmin()
        # real = 2 * yf[idx].real / n
        # imag = 2 * yf[idx].imag / n
        # amplitude = 2 * np.abs(yf[idx]) / n
        phase = np.angle(yf[idx])
        #results.append((freq, real, imag, amplitude, phase))
        
        # 计算指定频率附近±0.1Hz范围内的频率分量和
        nearby_indices = np.where((xf >= freq - bandwidth) & (xf <= freq + bandwidth))[0]
        nearby_real = np.sum(2 * yf[nearby_indices].real / n)
        nearby_imag = np.sum(2 * yf[nearby_indices].imag / n)
        nearby_amplitude = np.sum(2 * np.abs(yf[nearby_indices]) / n)
        
        # 振幅和相位保留空白
        results.append((freq, nearby_real, nearby_imag, nearby_amplitude,phase,'', ''))
    
    return results