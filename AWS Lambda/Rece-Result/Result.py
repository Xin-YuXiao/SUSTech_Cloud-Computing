import numpy as np

def result(receiver_amp, moniter_amp):
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
    
    # 转置矩阵以便按列存储
    combined_matrix = result_amp.reshape(-1, 1)
    
    # 定义文件名
    filename = 'ResultAmp.csv'
    
    # 写入注释行和数据
    with open(filename, 'w') as f:
        f.write(f'Result amp: {result}\n')
        np.savetxt(f, combined_matrix, fmt='%f')
    
    print(f'ReceiverAmp successfully written to {filename}')
    
    return result, result_amp

# 示例用法：
# receiver_amp = np.array([values])  # 从Receiver函数得出的电压幅值
# moniter_amp = np.array([values])  # 从Moniter函数得出的电流值

# result_value, result_amp = result(receiver_amp, moniter_amp)
