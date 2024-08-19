from utils import read_preprocessing_params, read_stable_current_file
import os
# 定义路径
PARAMS_FILE = r'C:/Users/xiaoyu/Desktop/data_nkd/pre.json'
OUTPUT_PATH = r'C:\\Users\\xiaoyu\\Desktop\\AWS Lambda\\output'  # 替换为实际的输出路径

# 创建输出文件夹（如果不存在）
if not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)

# 主函数
def main():
    params = read_preprocessing_params(PARAMS_FILE)
    required_keys = ['STACK_TIME_WINDOW', 'MINUTES_OF_ACTION', 'INPUT_PATH', 'OUTPUT_PATH', 'TX_ID', 'RX_ID']
    for key in required_keys:
        if key not in params:
            raise KeyError(f'Missing required parameter: {key}')
    print("Parameters loaded successfully.")
    return params

if __name__ == "__main__":
    params = main()
