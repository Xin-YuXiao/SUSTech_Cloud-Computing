# Pre.py
import os
import json
from PreTx import process_tx
from PreRx import process_rx

def load_config(file_path):
    print(f"Loading config file from {file_path}...")
    try:
        with open(file_path, 'r') as f:
            config = json.load(f)
        print("Config loaded successfully.")
        return config
    except Exception as e:
        print(f"Error loading config file: {e}")
        return {}

def start_preprocessing(input_path, output_path, tx_ids, rx_ids, stack_time_window, minutes_of_action):
    timeout = (min(minutes_of_action) - 1) * 60  # 下一个预处理时刻到来前1分钟
    
    for tx_id in tx_ids:
        print(f"Starting processing for TX_ID {tx_id}...")
        process_tx(input_path, output_path, tx_id, stack_time_window, minutes_of_action)
        
    for rx_id in rx_ids:
        for tx_id in tx_ids:  # 确保每个 RX_ID 对应一个 TX_ID
            print(f"Starting processing for RX_ID {rx_id} with TX_ID {tx_id}...")
            process_rx(input_path, output_path, rx_id, tx_id, stack_time_window, minutes_of_action, timeout)
        
    print("Processing completed for all tasks.")

def main():
    # 加载配置文件
    config_file_path = 'C:\\Users\\xiaoyu\\Desktop\\AWS Lambda\\pre.json'
    config = load_config(config_file_path)
    
    # 从配置文件中读取参数
    input_path = config.get('INPUT_PATH')
    output_path = config.get('OUTPUT_PATH')

    tx_ids = config.get('TX_ID')
    rx_ids = config.get('RX_ID')
    stack_time_window = config.get('STACK_TIME_WINDOW')
    minutes_of_action = config.get('MINUTES_OF_ACTION')
    
    # 确保输入目录存在
    if not os.path.exists(input_path):
        os.makedirs(input_path)
    
    # 确保输出目录存在
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    
    # 运行预处理函数
    start_preprocessing(input_path, output_path, tx_ids, rx_ids, stack_time_window, minutes_of_action)

if __name__ == "__main__":
    main()
