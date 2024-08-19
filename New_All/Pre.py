import os
import json
import time
import subprocess
from datetime import datetime, timezone
from typing import List

def read_parameters(json_file: str) -> dict:
    """从JSON文件中读取参数"""
    try:
        print(f"Reading parameters from {json_file}")
        with open(json_file, 'r', encoding='utf-8') as f:
            parameters = json.load(f)
        print(f"Parameters read: {parameters}")
        return parameters
    except FileNotFoundError:
        print(f"Error: JSON file {json_file} not found.")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON file {json_file}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while reading parameters: {e}")
    return {}

def find_subdirectory_with_id(directory: str, id_str: str) -> str:
    """查找包含指定ID的子目录"""
    try:
        print(f"Searching for subdirectory with ID {id_str} in {directory}")
        for subdir in os.listdir(directory):
            if id_str in subdir and os.path.isdir(os.path.join(directory, subdir)):
                found_dir = os.path.join(directory, subdir)
                print(f"Found subdirectory: {found_dir}")
                return found_dir
        print(f"No subdirectory found with ID {id_str}")
    except Exception as e:
        print(f"An error occurred while searching for subdirectory: {e}")
    return None

def run_pre_rx(rx_id: str, base_directory: str, output_directory: str, tx_id: str):
    """运行 PreRx.py"""
    print(f"Running PreRx.py for RX_ID {rx_id} with TX_ID {tx_id}")
    crt_filepath = get_latest_file(output_directory, tx_id, ".crt")
    if crt_filepath and os.path.exists(crt_filepath):
        try:
            print(f"Using CRT file: {crt_filepath}")
            result = subprocess.run(["python", "PreRx.py", str(rx_id), base_directory, output_directory, crt_filepath],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', timeout=300)
            if result.returncode == 0:
                print(f"VSB, VPM, and VSK files generated for RX_ID {rx_id} using CRT {crt_filepath}")
            else:
                print(f"PreRx.py for RX_ID {rx_id} failed with return code {result.returncode}: {result.stderr}")
        except subprocess.TimeoutExpired:
            print(f"PreRx.py for RX_ID {rx_id} timed out.")
        except Exception as e:
            print(f"Error running PreRx.py for RX_ID {rx_id}: {e}")
    else:
        print(f"No CRT file found for RX_ID {rx_id} with TX_ID {tx_id}")

def run_pre_tx(tx_id: str, base_directory: str, output_directory: str, stack_time_window: int) -> bool:
    """运行 PreTx.py"""
    print(f"Running PreTx.py for TX_ID {tx_id}")
    tx_id_str = str(tx_id)
    try:
        result = subprocess.run(["python", "PreTx.py", tx_id_str, base_directory, output_directory, str(stack_time_window)],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', timeout=300)
        if result.returncode == 0:
            print(f"CRT file generated for TX_ID {tx_id}")
            return True
        else:
            print(f"PreTx.py for TX_ID {tx_id} failed with return code {result.returncode}: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        print(f"PreTx.py for TX_ID {tx_id} timed out.")
    except Exception as e:
        print(f"Error running PreTx.py for TX_ID {tx_id_str}: {e}")
    return False

def get_latest_file(directory: str, file_id: str, extension: str) -> str:
    """获取最新的指定文件"""
    try:
        print(f"Getting latest file with ID {file_id} and extension {extension} in {directory}")
        files = find_files_with_id(directory, str(file_id), extension)
        if not files:
            print(f"No files found with ID {file_id} and extension {extension}")
            return None
        files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        latest_file = files[0]
        print(f"Latest file: {latest_file}")
        return latest_file
    except Exception as e:
        print(f"An error occurred while getting the latest file: {e}")
    return None

def find_files_with_id(directory: str, file_id: str, extension: str) -> List[str]:
    """查找特定ID的文件"""
    try:
        print(f"Finding files with ID {file_id} and extension {extension} in {directory}")
        file_id_str = str(file_id)
        files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(extension) and file_id_str in f]
        print(f"Found files: {files}")
        return files
    except Exception as e:
        print(f"An error occurred while finding files: {e}")
    return []

def preprocess_data(tx_ids: List[str], rx_ids: List[str], base_directory: str, output_directory: str, stack_time_window: int):
    """预处理数据的逻辑"""
    print(f"Starting data preprocessing for TX_IDs {tx_ids} and RX_IDs {rx_ids}")
    for tx_id in tx_ids:
        print(f"Processing TX_ID {str(tx_id)}")
        tx_directory = find_subdirectory_with_id(base_directory, str(tx_id))
        if tx_directory:
            print(f"TX_ID {str(tx_id)} found, running PreTx.py")
            if run_pre_tx(str(tx_id), base_directory, output_directory, stack_time_window):
                for rx_id in rx_ids:
                    print(f"Running PreRx.py for RX_ID {str(rx_id)}")
                    run_pre_rx(str(rx_id), base_directory, output_directory, str(tx_id))
        else:
            print(f"TX_ID {str(tx_id)} not found, skipping PreTx.py and only running PreRx.py")
            for rx_id in rx_ids:
                print(f"Running PreRx.py for RX_ID {str(rx_id)}")
                run_pre_rx(str(rx_id), base_directory, output_directory, str(tx_id))

def main_loop(base_directory: str, output_directory: str, minutes_of_action: List[int], tx_ids: List[str], rx_ids: List[str], stack_time_window: int):
    """主循环，定期检查并处理数据"""
    print("Starting main loop")
    processed_minutes = set()
    while True:
        current_time = datetime.now(timezone.utc)
        current_minute = current_time.minute
        print(f"Current time: {current_time}")
        if current_minute in minutes_of_action and current_minute not in processed_minutes:
            print(f"Minute {current_minute} is in MINUTES_OF_ACTION, processing...")
            preprocess_data(tx_ids, rx_ids, base_directory, output_directory, stack_time_window)
            processed_minutes.add(current_minute)
            time.sleep(60)  # 避免在同一分钟内多次执行
        elif current_minute not in minutes_of_action:
            print(f"Minute {current_minute} is not in MINUTES_OF_ACTION, skipping...")
            processed_minutes.clear()  # 重置已处理的分钟记录
        time.sleep(30)  # 每 30 秒检查一次时间

if __name__ == "__main__":
    json_file = r'C:\Users\xiaoyu\Desktop\AWS Lambda\pre.json'
    print(f"Reading parameters from {json_file}")
    params = read_parameters(json_file)

    if params:
        base_directory = params.get("INPUT_PATH", "")
        output_directory = params.get("OUTPUT_PATH", "")
        minutes_of_action = params.get("MINUTES_OF_ACTION", [])
        stack_time_window = params.get("STACK_TIME_WINDOW", 0)

        tx_ids = params.get("TX_ID", [])
        rx_ids = params.get("RX_ID", [])

        if not all([base_directory, output_directory, minutes_of_action, stack_time_window, tx_ids, rx_ids]):
            print("Error: Missing necessary parameters in JSON file.")
        else:
            print(f"Starting main loop with parameters: base_directory={base_directory}, output_directory={output_directory}, minutes_of_action={minutes_of_action}, tx_ids={tx_ids}, rx_ids={rx_ids}")
            main_loop(base_directory, output_directory, minutes_of_action, tx_ids, rx_ids, stack_time_window)
    else:
        print("Error: Failed to read parameters from JSON file.")
