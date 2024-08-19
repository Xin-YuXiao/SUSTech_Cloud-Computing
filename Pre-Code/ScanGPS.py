import os
import time
import xml.etree.ElementTree as ET
import pandas as pd

# 定义扫描的目录和输出文件路径
SCAN_DIRECTORY = 'C:\\Users\\xiaoyu\\Desktop\\data_nkd'  # 确保这是一个目录
GPS_CSV_FILE = 'C:\\Users\\xiaoyu\\Desktop\\data_nkd\\gps.csv'
SCAN_INTERVAL = 600  # 扫描间隔时间，单位为秒

# 解析 XML 文件中的所有信息
def parse_info_from_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    info_data = []

    for adsr in root.findall('ADSR'):
        info = {
            'line_no': adsr.find('line_no').text,
            'station_no': adsr.find('station_no').text,
            'ADSR_serial_no': adsr.find('ADSR_serial_no').text,
            'Battery_sn': adsr.find('Battery_sn').text,
            'source': adsr.find('source').text,
            'deployment_status': adsr.find('deployment_status').text,
            'deployment_time': adsr.find('deployment_time').text,
            'dep_latitude': adsr.find('dep_latitude').text,
            'dep_longitude': adsr.find('dep_longitude').text,
            'distance': adsr.find('distance').text,
            'dep_operator': adsr.find('dep_operator').text,
            'comments': adsr.find('comments').text
        }
        info_data.append(info)

    return info_data

# 更新 CSV 文件
def update_csv(file_path, data, columns):
    df = pd.DataFrame(data, columns=columns)
    if os.path.exists(file_path):
        existing_df = pd.read_csv(file_path)
        df = pd.concat([existing_df, df]).drop_duplicates(subset=['ADSR_serial_no'], keep='last')
    df.to_csv(file_path, index=False)

# 扫描 XML 文件并更新 GPS CSV 文件
def scan_and_update_gps():
    for filename in os.listdir(SCAN_DIRECTORY):
        if filename.endswith('.xml'):
            file_path = os.path.join(SCAN_DIRECTORY, filename)
            info_data = parse_info_from_xml(file_path)
            update_csv(GPS_CSV_FILE, info_data, [
                'line_no', 'station_no', 'ADSR_serial_no', 'Battery_sn', 'source',
                'deployment_status', 'deployment_time', 'dep_latitude', 'dep_longitude',
                'distance', 'dep_operator', 'comments'
            ])
            for info in info_data:
                print(f"Scanned and updated GPS data for ADSR_serial_no: {info['ADSR_serial_no']}")

def main():
    while True:
        print("Starting new scan cycle...")
        scan_and_update_gps()
        print("Scan cycle completed. All files updated.")
        print(f"Sleeping for {SCAN_INTERVAL // 60} minutes...")
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    main()
