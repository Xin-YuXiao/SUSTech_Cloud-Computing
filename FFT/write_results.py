# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 15:44:48 2024

@author: WDY
"""
import csv

# 写入结果到CSV文件
def write_results_to_csv(filename, results):
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Frequency", "Real", "Imaginary", "Amplitude", "Phase"])
        for result in results:
            writer.writerow([str(item) for item in result])
   
# 写入结果到txt文件
def write_results_to_txt(filename, results):
    with open(filename, 'w') as f:
        # 写入表头
        f.write("{:<12} {:<15} {:<15} {:<15} {:<15}\n".format("Frequency", "Real", "Imaginary", "Amplitude", "Phase"))
        
        # 写入数据
        for result in results:
            f.write("{:<12.6f} {:<15.6f} {:<15.6f} {:<15.6f} {:<15.6f}\n".format(result[0], result[1], result[2], result[3], result[4]))