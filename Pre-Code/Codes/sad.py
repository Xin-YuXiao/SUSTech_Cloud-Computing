import os
import glob

def delete_csv_files(directory):
    try:
        # 查找目录中的所有.csv文件
        csv_files = glob.glob(os.path.join(directory, '*.csv'))
        
        # 遍历并删除每个.csv文件
        for csv_file in csv_files:
            os.remove(csv_file)
            print(f"已删除文件: {csv_file}")

        print("所有.csv文件已删除。")
    except Exception as e:
        print(f"删除.csv文件时发生错误: {e}")

# 指定目录路径
directory_path = r'C:\Users\xiaoyu\Desktop\data_nkd\data2'  # 替换为你实际的目录路径

# 调用函数删除.csv文件
delete_csv_files(directory_path)
