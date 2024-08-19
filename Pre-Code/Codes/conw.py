import numpy as np
import pandas as pd
from datetime import datetime

def receiver(data, time, peak, moniter_amp):
    """
    计算接收电流的幅值并保存到CSV文件

    参数:
    data : numpy数组, 输入数据
    time : datetime对象, 数据时间戳
    peak : numpy数组, 极值索引点
    moniter_amp : numpy数组, 电流值

    返回:
    receiver_amp : numpy数组, 归一化后的电压幅值
    """
    # 找到moniter_amp中非0元素
    id = np.where(moniter_amp != 0)
    moniter_amp[id] = 1.0 / moniter_amp[id]  # 按照与1a之间的比值运算，得到归一化因子，用于后续归一化
    receiver_amp = moniter_amp.copy()
    
    for i in range(len(peak)):
        if moniter_amp[i] != 0:
            receiver_amp[i] = moniter_amp[i] * np.mean(data[peak[0, i]:peak[1, i]])
        else:
            receiver_amp[i] = np.mean(data[peak[0, i]:peak[1, i]])

    # 合并向量
    combined_matrix = np.vstack((peak, receiver_amp))
    
    # 转置矩阵以便按列存储
    combined_matrix = combined_matrix.T
    
    # 定义文件名
    filename = 'ReceiverData.csv'
    
    # 提取年月日时分部分并格式化为 'YYYYMMDDHHMM' 形式
    date_time_part = time.strftime('%Y%m%d%H%M')
    
    # 提取秒部分
    seconds_part = time.strftime('%S')
    
    # 打开文件
    with open(filename, 'w') as f:
        # 写入注释行
        f.write(f'{date_time_part},{seconds_part},0.001s\n')
        
        # 逐行写入数据
        for row in combined_matrix:
            f.write(', '.join(map(str, row)) + '\n')
    
    print(f'Data successfully written to {filename}')
    return receiver_amp

def remove_outliers_new(data, max_iterations=10, threshold=3):
    """
    去除数据中的离群点并返回均值和索引值

    参数:
    data : numpy数组, 输入数据
    max_iterations : int, 最大迭代次数
    threshold : int, 离群点判断阈值，默认3倍标准差

    返回:
    mean_value : float, 去除离群点后的均值
    indices : numpy数组, 保留的数据点索引
    """
    data = np.array(data).flatten()
    n = len(data)
    
    # 初始均值和标准差
    mean_value = np.mean(data)
    std_value = np.std(data)
    
    # 创建一个初始的索引数组
    indices = np.arange(n)
    
    # 设置最大循环次数
    iteration_count = 0
    
    # 从两端向中间去除离群点
    while True:
        # 更新循环计数器
        iteration_count += 1
        
        # 计算与当前均值的差距
        diff = np.abs(data - mean_value)
        
        # 找到最大差距的索引
        outlier_index = np.argmax(diff)
        
        # 判断是否超过一定数量的标准差
        if diff[outlier_index] <= threshold * std_value or iteration_count > max_iterations:
            break
        
        # 从左侧或右侧删除数据点，直到索引超过当前离群点
        if outlier_index <= len(data) // 2:
            # 左侧删除数据点
            data = data[outlier_index:]
            indices = indices[outlier_index:]
        else:
            # 右侧删除数据点
            data = data[:outlier_index + 1]
            indices = indices[:outlier_index + 1]
        
        # 更新均值和标准差
        mean_value = np.mean(data)
        std_value = np.std(data)
        
        # 更新n
        n = len(data)
        
        # 检查是否剩余的数据点过少
        if n <= 1:
            break
    
    return mean_value, indices

def result(receiver_amp, moniter_amp):
    """
    计算提取接收电流的幅值并保存到CSV文件

    参数:
    receiver_amp : numpy数组, Receiver函数得出的归一化后的电压幅值
    moniter_amp : numpy数组, 使用Moniter函数得出的电流值

    返回:
    result : float, 最终计算得出的电压值
    result_amp : numpy数组, 每次激发记录到的归一化的电压值
    """
    if len(moniter_amp) % 2 == 1:
        moniter_amp = moniter_amp[:-1]

    result_amp = np.zeros(len(moniter_amp) // 2)
    num = 0
    for i in range(0, len(moniter_amp), 2):
        result_amp[num] = receiver_amp[i] - receiver_amp[i + 1]
        num += 1

    result = np.mean(np.abs(result_amp))
    if receiver_amp[0] * moniter_amp[0] < 0:
        result = -result

    # 定义文件名
    filename = 'ResultAmp.csv'

    # 打开文件并写入数据
    with open(filename, 'w') as f:
        # 写入注释行
        f.write(f'Result amp: {result}\n')

        # 逐行写入数据
        for value in result_amp:
            f.write(f'{value}\n')

    print(f'ReceiverAmp successfully written to {filename}')

    return result, result_amp

# 示例调用
data = np.random.randn(1000)  # 示例数据
time = datetime.now()
peak = np.array([[100, 200], [300, 400]])  # 示例峰值索引
moniter_amp = np.array([0.5, 1.0])  # 示例监控幅值

# 计算接收电流的幅值
receiver_amp = receiver(data, time, peak, moniter_amp)

# 去除离群点并获取均值和索引
mean_value, indices = remove_outliers_new(data)
print(f"去除离群点后的均值: {mean_value}")
print(f"保留的数据点索引: {indices}")

# 计算最终电压值并保存结果
result_value, result_amp = result(receiver_amp, moniter_amp)
print(f"最终计算得出的电压值: {result_value}")
print(f"每次激发记录到的归一化的电压值: {result_amp}")
