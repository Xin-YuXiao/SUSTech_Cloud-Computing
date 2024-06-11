import numpy as np
import pandas as pd
from datetime import datetime

def receiver(data, time, Peak, MoniterAmp):
    # 找到MoniterAmp中非0元素
    id_nonzero = MoniterAmp != 0
    MoniterAmp[id_nonzero] = 1.0 / MoniterAmp[id_nonzero]  # 归一化因子，用于后续归一化

    ReceiverAmp = MoniterAmp.copy()
    for i in range(len(Peak[0])):
        if MoniterAmp[i] != 0:
            ReceiverAmp[i] = MoniterAmp[i] * np.mean(data[int(Peak[0][i]):int(Peak[1][i])])
        else:
            ReceiverAmp[i] = np.mean(data[int(Peak[0][i]):int(Peak[1][i])])

    # 合并向量
    combined_matrix = np.vstack((Peak, ReceiverAmp))

    # 转置矩阵以便按列存储
    combined_matrix = combined_matrix.T

    # 定义文件名
    filename = 'ReceiverData.csv'

    # 提取年月日时分部分并格式化为 'YYYYMMDDHHMM' 形式
    date_time_part = time.strftime('%Y%m%d%H%M')
    seconds_part = time.strftime('%S')

    # 写入注释行和数据
    with open(filename, 'w') as f:
        f.write(f"{date_time_part},{seconds_part},0.001s\n")
        np.savetxt(f, combined_matrix, delimiter=", ", fmt="%f")

    print(f"Data successfully written to {filename}")

    return ReceiverAmp

# 示例用法：
# 假设data是从csv文件读取的时间序列数据
# time是数据记录的时间
# Peak是使用Moniter函数得出的极值索引点
# MoniterAmp是使用Moniter函数得出的电流值
# data = pd.read_csv('data.csv')['column_name'].values
# time = datetime.now()  # 这个值应当从数据中提取
# Peak = np.array([[start_indices], [end_indices]])  # 这是一个示例
# MoniterAmp = np.array([values])  # 这是一个示例

# ReceiverAmp = receiver(data, time, Peak, MoniterAmp)
