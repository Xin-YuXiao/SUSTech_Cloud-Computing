import numpy as np

def remove_outliers(data):
    data = np.array(data).flatten()  # 将数据转换为列向量
    n = len(data)
    k = 5  # 每次去掉5个离群点
    if k >= n:
        raise ValueError('k should be less than the number of elements in the array')

    mean_value = np.mean(data)  # 初始均值
    indices = np.arange(n)  # 创建一个初始的索引数组

    # 从两端向中间去除离群点
    while True:
        diff = np.abs(data - mean_value)  # 计算与当前均值的差距
        outlier_index = np.argmax(diff)  # 找到最大差距的索引

        new_mean_value = np.mean(data)
        if abs((diff[outlier_index] - abs(new_mean_value)) / new_mean_value) <= 0.5:
            break

        # 从左侧或右侧删除k个数据点
        if outlier_index <= n / 2:
            # 左侧删除k个数据点
            data = data[k:]
            indices = indices[k:]
        else:
            # 右侧删除k个数据点
            data = data[:-k]
            indices = indices[:-k]

        mean_value = np.mean(data)  # 更新均值
        n = len(data)  # 更新n

        if n <= k:  # 检查是否剩余的数据点少于k个
            break

    return mean_value, indices

# 示例用法：
# data = np.array([1, 2, 3, 100, 5, 6, 7, 200, 9, 10])
# mean_value, indices = remove_outliers(data)
# print("Mean value:", mean_value)
# print("Indices:", indices)
