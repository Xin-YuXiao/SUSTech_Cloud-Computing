import json
import time
import boto3
import numpy as np
from matplotlib import pyplot as plt
from makeRectMeshModelBlocks import makeRectMeshModelBlocks

# AWS S3 and Lambda configuration
bucket_name = 'inputdataset1'
data_file_name = 'data1.json'
result_file_name = 'processed-data1.json'
lambda_function_name = 'resnet-0'

# Generate data
def generate_data():
    h = 2
    ratio = 1.14
    nctbc = 30
    tmp = np.cumsum(h * np.power(ratio, np.arange(nctbc + 1)))
    nodeX = np.round(np.concatenate((-tmp[::-1], [0], tmp)))
    nodeY = np.round(np.concatenate((-tmp[::-1], [0], tmp)))
    nodeZ = np.round(np.concatenate(([0], -tmp)))

    blkLoc = np.array([-np.inf, np.inf, -np.inf, np.inf, 0, -np.inf])
    blkCon = np.array([1e-2])
    cellCon, faceCon, edgeCon = makeRectMeshModelBlocks(nodeX, nodeY, nodeZ, blkLoc, blkCon, [], [], [])

    tx = np.array([[(0, 0, 0, 1), [-np.inf, 0, 0, -1]]])
    rx = np.array([[[10, 0, 0, 20, 0, 0],
                    [20, 0, 0, 30, 0, 0],
                    [30, 0, 0, 40, 0, 0],
                    [40, 0, 0, 50, 0, 0],
                    [50, 0, 0, 60, 0, 0],
                    [60, 0, 0, 70, 0, 0],
                    [70, 0, 0, 80, 0, 0],
                    [80, 0, 0, 90, 0, 0],
                    [90, 0, 0, 100, 0, 0]]])

    data = {
        'nodeX': nodeX.tolist(),
        'nodeY': nodeY.tolist(),
        'nodeZ': nodeZ.tolist(),
        'edgeCon': edgeCon.tolist(),
        'faceCon': faceCon.tolist(),
        'cellCon': cellCon.tolist(),
        'tx': tx.tolist(),
        'rx': rx.tolist()
    }
    
    return data, rx

# Upload data to S3
def upload_to_s3(data, bucket_name, file_name):
    s3 = boto3.client('s3')
    s3.put_object(Body=json.dumps(data), Bucket=bucket_name, Key=file_name)
    print(f"Data uploaded to S3 bucket {bucket_name} with key {file_name}.")

# Trigger Lambda function
def trigger_lambda(lambda_function_name, payload):
    lambda_client = boto3.client('lambda')
    response = lambda_client.invoke(
        FunctionName=lambda_function_name,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )
    response_payload = json.loads(response['Payload'].read())
    print(f"Lambda function {lambda_function_name} triggered. Response: {response_payload}")
    return response_payload

# Download results from S3
def download_from_s3(bucket_name, file_name, local_file_path):
    s3 = boto3.client('s3')
    try:
        s3.download_file(bucket_name, file_name, local_file_path)
        print(f"Data downloaded from S3 bucket {bucket_name} with key {file_name} to {local_file_path}.")
    except boto3.exceptions.S3UploadFailedError as e:
        print(f"Failed to download file from S3: {e}")
    except s3.exceptions.NoSuchKey as e:
        print(f"The file {file_name} does not exist in the bucket {bucket_name}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Process and plot results
def process_and_plot(local_file_path, rx):
    with open(local_file_path, 'r') as f:
        data = json.load(f)
    
    potentials = np.array(data['potentials'])
    dV = np.array(data['data'])

    '''Compare against analytic solutions'''
    Aloc = np.array([0, 0, 0])
    rAM = rx[0][:, 0] - Aloc[0]
    rAN = rx[0][:, 3] - Aloc[0]
    rho = 100
    I = 1
    dV_analytic = rho * I / 2 / np.pi * (1 / rAM - 1 / rAN)
    X = 0.5 * (rx[0][:, 0] + rx[0][:, 3])

    fig, axs = plt.subplots(2, 1, figsize=(10, 10))
    axs[0].semilogy(X, dV[0], '.-', label='RESnet')
    axs[0].plot(X, dV_analytic, 'o-', label='Analytic', markerfacecolor='none')
    axs[0].set_title('(a) Numerical and analytic solutions')
    axs[0].set_xlabel('Tx-Rx offset (m)')
    axs[0].set_ylabel('Potential difference (V)')
    axs[0].set_xlim(10, 100)
    axs[0].set_ylim(0.01, 1)
    axs[0].legend()
    axs[0].grid(True)

    axs[1].plot(X, ((dV[0] - dV_analytic) / dV_analytic), 'k.-')
    axs[1].set_title('(b) Numerical errors')
    axs[1].set_xlabel('Tx-Rx offset (m)')
    axs[1].set_ylabel('Relative error')
    axs[1].set_xlim(10, 100)
    axs[1].set_ylim(-0.03, 0.02)
    axs[1].grid(True)

    plt.show()

# Main function to integrate the entire workflow
def main():
    # Step 1: Generate data
    data, rx = generate_data()
    
    # Step 2: Upload generated data to S3
    upload_to_s3(data, bucket_name, data_file_name)
    
    # Step 3: Trigger Lambda function
    lambda_payload = {'bucket_name': bucket_name, 'file_name': data_file_name}
    trigger_lambda(lambda_function_name, lambda_payload)
    
    # Step 4: Wait for Lambda function to complete (this can be improved with event notifications)
    time.sleep(10)  # 增加等待时间，确保Lambda函数完成
    
    # Step 5: Download results from S3
    local_file_path = 'downloaded_data.json'
    if check_file_exists(bucket_name, 'processed-data1.json'):
        download_from_s3(bucket_name, 'processed-data1.json', local_file_path)
    
        # Step 6: Process and plot results
        process_and_plot(local_file_path, rx)
    else:
        print("File not found, aborting download.")

# Function to check if a file exists in S3
def check_file_exists(bucket_name, file_name):
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket_name, Key=file_name)
        print(f"File {file_name} exists in bucket {bucket_name}.")
        return True
    except s3.exceptions.ClientError as e:
        print(f"File {file_name} does not exist in bucket {bucket_name}: {e}")
        return False

if __name__ == "__main__":
    main()
