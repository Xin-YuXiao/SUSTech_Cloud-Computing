import os
import numpy as np
from obspy import read
from scipy.stats import zscore

def remove_outliers(data, threshold=3):
    """
    去除离群点并计算均值
    参数:
        data: 时间序列数据
        threshold: 离群点判断的z-score阈值
    返回:
        均值和去除离群点后的索引
    """
    z_scores = zscore(data)
    filtered_indices = np.where(np.abs(z_scores) < threshold)[0]
    return np.mean(data[filtered_indices]), filtered_indices

def monitor(data, hall_coefficient):
    """
    提取监测电流的幅值和极点点位
    参数:
        data: 时间序列数据
        hall_coefficient: 电压转换为电流的转换系数
    返回:
        极值点和DC幅值
    """
    window_size = 20  # 窗口大小
    num_windows = len(data) - window_size + 1  # 窗口数目

    # 初始化结果数组
    data_y = np.zeros(num_windows)
    add_value = np.zeros(19)

    # 遍历每个窗口并计算均值的差值
    for i in range(num_windows):
        idx = slice(i, i + window_size)  # 当前窗口的滑动索引
        if i + 2 * window_size > len(data):
            idy = idx
        else:
            idy = slice(i + window_size, i + 2 * window_size)
        data_y[i] = np.mean(data[idy]) - np.mean(data[idx])  # 计算当前窗口的均值

    add_value[:] = np.mean(data_y)
    data_y = np.concatenate((add_value, data_y))
    absnum = np.max(data_y) / 2
    # 将data数组中小于absnum的数值赋值为0
    data_y[np.abs(data_y) < absnum] = 0

    # 找到所有极值
    peak_sort = np.zeros(len(data_y))
    for i in range(1, len(data_y) - 1):
        if (data_y[i] - data_y[i - 1]) * (data_y[i + 1] - data_y[i]) < 0:
            peak_sort[i] = i

    peak_sort = peak_sort[peak_sort != 0]  # 将peak_sort中0点去掉，只保留极值点

    # 由于背景场波动，提取极值点时会有临近点误识别为极值点
    # 需要判断两点之间是否相隔过近
    peak = np.zeros(len(peak_sort))
    for i in range(len(peak_sort) - 1):
        if peak_sort[i + 1] - peak_sort[i] < 20:
            peak[i] = peak_sort[i]

    peak = peak[peak != 0]  # 删除为0的元素
    peak_sort = np.setdiff1d(peak_sort, peak)  # 删除相同的元素

    # 将 peak_sort 转换为整数索引
    peak_sort = peak_sort.astype(int)

    # 根据提取出的极值点，选择合适的范围
    dc_amp = np.zeros(len(peak_sort) - 1)
    peak_indices = np.zeros((2, len(peak_sort) - 1))

    for i in range(len(peak_sort) - 1):
        mean_value, indices = remove_outliers(data[peak_sort[i]:peak_sort[i + 1]])
        dc_amp[i] = mean_value
        peak_indices[0, i] = peak_sort[i] + np.min(indices) + 5
        peak_indices[1, i] = peak_sort[i] + np.max(indices) - 5

    dc_amp[np.abs(dc_amp) < (np.max(dc_amp) / 2)] = 0  # 将DC_amp中无激发时的电流幅值归为0

    dc_amp = dc_amp * hall_coefficient  # 乘以转换系数，将电压转换为电流
    peak = peak_indices
    return peak, dc_amp

def read_miniseed(file_path):
    """
    读取MiniSEED文件
    参数:
        file_path: MiniSEED文件路径
    返回:
        时间序列数据和时间信息
    """
    st = read(file_path)
    tr = st[0]
    data = tr.data
    start_time = tr.stats.starttime.strftime('%Y%m%d%H%M')
    sampling_rate = tr.stats.sampling_rate
    start_seconds = int(tr.stats.starttime.strftime('%S'))  # 起算点的秒数
    return data, start_time, start_seconds, sampling_rate

# 读取miniseed文件
file_path = r"C:\Users\xiaoyu\Desktop\data2\600000002.20240513.090300000.Z.miniseed"  # 根据实际文件路径修改
data, start_time, start_seconds, sampling_rate = read_miniseed(file_path)
hall_coefficient = 1.0  # 示例转换系数
peak, dc_amp = monitor(data, hall_coefficient)

print("Peak:", peak)
print("DC_amp:", dc_amp)

# 定义桌面路径和文件夹路径
desktop_path = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
moniter_folder_path = os.path.join(desktop_path, 'Moniter')

# 如果文件夹不存在，创建它
if not os.path.exists(moniter_folder_path):
    os.makedirs(moniter_folder_path)

# 找到文件夹中已有文件的数量
existing_files = len([name for name in os.listdir(moniter_folder_path) if os.path.isfile(os.path.join(moniter_folder_path, name))])
file_index = existing_files + 1

# 定义保存文件的完整路径
save_path = os.path.join(moniter_folder_path, f'moniter_{file_index}.txt')

# 将数据保存为txt文件，三列形式
with open(save_path, 'w') as f:
    # 写入第一行的绝对时间、起算点和采样间隔信息
    f.write(f"{start_time}\t{start_seconds:02d}\t输入参数（采样率）\n")
    # 写入后续行的峰值点和幅值信息
    for i in range(len(dc_amp)):
        f.write(f"{peak[0, i]}\t{peak[1, i]}\t{dc_amp[i]:.6f}\n")

print(f"Results saved to {save_path}")
