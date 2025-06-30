import requests
import re
def meta(key):
    packet = {
        "1":"https://storage.googleapis.com/cloud-training/T-STREAM-I/2.5/od/en/student/OD_M1_Introduction_to_Processing_Streaming_Data.pdf",
        "2":"https://storage.googleapis.com/cloud-training/T-STREAM-I/2.5/od/en/student/OD_M2_Serverless_Messaging_with_Pub_Sub.pdf",
        "3":"https://storage.googleapis.com/cloud-training/T-STREAM-I/2.5/od/en/student/OD_M3_Dataflow_Streaming_Features.pdf",
        "4":"https://storage.googleapis.com/cloud-training/T-STREAM-I/2.5/od/en/student/OD_M4_High-Throughput_BigQuery_and_Bigtable_Streaming_Features.pdf",
        "5":"https://storage.googleapis.com/cloud-training/T-STREAM-I/2.5/od/en/student/OD_M5_Advanced_BigQuery_Functionality_and_Performance.pdf",
        "6":"https://storage.googleapis.com/cloud-training/T-STREAM-I/2.5/od/en/student/OD_M6_Summary.pdf"
        }
    return packet[key]

def download_file(url,key):
    module  = f"06_0{key}_BuildingResilientStreamingAnalyticsSystems"
    pattern = re.compile(r"_[Mm]\d+_")
    filename =  re.split(pattern, url.split("/")[-1])
    filename = f"{module}_{filename[-1].replace("_","")}"
    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded successfully: {filename}")
    else:
        print(f"Failed to download. Status code: {response.status_code}")

if __name__ == '__main__':
    keys = [str(num) for num in range(1,7)]
    for key in keys:
        urls = meta(key)
        download_file(urls,key)

