import requests
import zipfile
import json
import os
from hdfs import InsecureClient

# Step 1: Download the zip file
url = "https://www.sec.gov/edgar/sec-api-documentation"
zip_filename = "data.zip"

response = requests.get(url)
if response.status_code == 200:
    with open(zip_filename, "wb") as f:
        f.write(response.content)
else:
    print("Failed to download the file. Status code:", response.status_code)
    exit(1)

# Step 2: Extract JSON files
extracted_folder = "extracted_files"
if not os.path.exists(extracted_folder):
    os.makedirs(extracted_folder)

with zipfile.ZipFile(zip_filename, "r") as zip_ref:
    zip_ref.extractall(extracted_folder)

# Step 3: Parse JSON files
json_data = []
for filename in os.listdir(extracted_folder):
    if filename.endswith(".json"):
        with open(os.path.join(extracted_folder, filename), "r") as json_file:
            data = json.load(json_file)
            json_data.append(data)

# Step 4: Store data in HDFS
hdfs_host = "http://your-hdfs-hostname:50070"
hdfs_user = "your-hdfs-username"
client = InsecureClient(hdfs_host, user=hdfs_user)

hdfs_path = f'/user/{hdfs_user}/json_data'
client.makedirs(hdfs_path)

for i, data in enumerate(json_data):
    json_filename = f"file_{i}.json"
    with client.write(f'{hdfs_path}/{json_filename}', encoding='utf-8') as writer:
        json.dump(data, writer)

# Step 5: View data through HIVE
os.system('hive -e "SELECT * FROM your_hive_table_name;"')
