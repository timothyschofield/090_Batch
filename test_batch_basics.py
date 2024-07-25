"""
File : test_batch_basics.py

Author: Tim Schofield
Date: 21 July 2024
    
https://platform.openai.com/docs/api-reference/batch

input_file_id: The ID of an uploaded file that contains requests for the new batch.
    Your input file must be formatted as a JSONL file, and must be uploaded with the purpose batch. 
    The file can contain UP TO 50,000 REQUESTS, and can be up to 100 MB in size.

endpoint: The endpoint to be used for ALL REQUESTS IN THE BATCH.

Supported endpoint:

    /v1/chat/completions
    /v1/embeddings
    /v1/completions
    
My call is usualy
ocr_output = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload) 
    
completion_window: The time frame within which the batch should be processed. 
    Currently only 24h is supported. - but for a small batch it might be completed in 2 minutes - you have to check the completion status of the batch every few minutes
    
metadata: Optional custom metadata for the batch. << what is this?
--------------------------------

# This is the correct structure of a jsonl
{
    "custom_id": "request-0",
    "method": "POST",
    "url": "/v1/chat/completions",
    "body": {
        "model": "gpt-4o-mini",
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Read this herbarium sheet and return the text."
                    },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": "http://sweetgum.nybg.org/images3/4220/762/04285750.jpg"
                        }
                    }
                ]
            }
        ],
        "max_tokens": "8192"
    }
}

"""
import openai
from openai import OpenAI
from dotenv import load_dotenv
from helper_functions_batch import get_file_timestamp,are_keys_valid, get_headers, save_dataframe_to_csv

import requests
import os
from pathlib import Path 
import numpy as np
import pandas as pd
import time
import json
import sys

import datetime
print(f"Python version {sys.version}")

print(1721658713 - 1721658283) # seconds 430    - 3 * 143 secs each
print(1721662364 - 1721661677) # seconds 687    - 10 * 68 secs each
print(1721665507 - 1721665298) # seconds 209    - 100 * 2 seconds each - impossible
"""
Batch(id='batch_elUmiErkxQvHScS2kNSF1Vws', completion_window='24h', created_at=1721665298, endpoint='/v1/chat/completions', 
      input_file_id='file-hdFZ1P5wpvYvzGOj9ffvqsLq', object='batch', status='completed', cancelled_at=None, cancelling_at=None, completed_at=1721665507, error_file_id='file-doEI17y5iP2G6TbVTP1cWLj0', errors=None, expired_at=None, expires_at=1721751698, failed_at=None, finalizing_at=1721665503, 
      in_progress_at=1721665299, metadata=None, output_file_id='file-lXt6EchiDEH56Wto7nmi7kPR', request_counts=BatchRequestCounts(completed=96, failed=4, total=100))
"""

timestamp = 1721658713 # 2024-07-22 14:24:43 -> 2024-07-22 14:31:53
date_time = datetime.datetime.utcfromtimestamp(timestamp)
print(date_time)

# proper input and output folders



load_dotenv()
try:
    my_api_key = os.environ['OPENAI_API_KEY']          
    client = OpenAI(api_key=my_api_key)
except Exception as ex:
    print("Exception:", ex)
    exit()

# Returns a list of current batches
active_batches = client.batches.list()
#print(active_batches)
print("---------------------------------")
for batch in active_batches.data:
    print(f"id: {batch.id}, status: {batch.status}, {batch.request_counts}, output_file_id: {batch.output_file_id}")
print("---------------------------------")

"""
id: batch_elUmiErkxQvHScS2kNSF1Vws, status: completed, BatchRequestCounts(completed=96, failed=4, total=100), output_file_id: file-lXt6EchiDEH56Wto7nmi7kPR
id: batch_3JTjqA49oe6Lzg2zMAtND3zI, status: completed, BatchRequestCounts(completed=10, failed=0, total=10), output_file_id: file-oRQ8Dnm2hVemBHQn4zkTCJH3
id: batch_1Wdn2CwWdGXMyjyRlJB68Y7h, status: completed, BatchRequestCounts(completed=10, failed=0, total=10), output_file_id: file-JxK2KTgphePjxGBLFOl1PJKS
"""
# Deleteing batches

# 1) Uploads the file with the batch of OpenAI requests - one on each line
batch_upload_response = client.files.create(file=open("my_image_batch100.jsonl", "rb"), purpose="batch")
print(batch_upload_response)
print("---------------------------------")
# FileObject(id='file-cblacIDHGepXzvqnrP8gXisn', bytes=1169, created_at=1721591563, filename='my_batch_requests.jsonl', object='file', purpose='batch', status='processed', status_details=None)

# 2) Create a batch processing request for the above uploaded file using the file id returned when you uploaded the file
# NameError: 
batch_create_response = client.batches.create(input_file_id=batch_upload_response.id, endpoint="/v1/chat/completions", completion_window="24h")
print(batch_create_response)
print("---------------------------------")
"""
Batch(id='batch_BWFV2g9dRhkREYA7Xhd1kCaE', completion_window='24h', created_at=1721634817, endpoint='/v1/chat/completions', input_file_id='file-IrEATWJzlEWp4F0vwrBuY8mP', 
object='batch', status='validating', cancelled_at=None, cancelling_at=None, completed_at=None, error_file_id=None, errors=None, expired_at=None, expires_at=1721721217, 
failed_at=None, finalizing_at=None, in_progress_at=None, metadata=None, output_file_id=None, request_counts=BatchRequestCounts(completed=0, failed=0, total=0))
"""

# openai.NotFoundError
# 3) Check how the batch is doing. If status='completed' - get the output file
batch_get_info_response = client.batches.retrieve('batch_elUmiErkxQvHScS2kNSF1Vws')
print(batch_get_info_response)
print(f"{batch_get_info_response.completed_at - batch_get_info_response.created_at} seconds")
print("---------------------------------")
"""
1 minutes later
Batch(id='batch_BWFV2g9dRhkREYA7Xhd1kCaE', completion_window='24h', created_at=1721634817, endpoint='/v1/chat/completions', 
input_file_id='file-IrEATWJzlEWp4F0vwrBuY8mP', object='batch', status='completed', cancelled_at=None, cancelling_at=None, completed_at=1721634892, 
error_file_id=None, errors=None, expired_at=None, expires_at=1721721217, failed_at=None, finalizing_at=1721634892, in_progress_at=1721634818, 
metadata=None, output_file_id='file-jTmU4j9f3YVV88qYeDePUSJ8', request_counts=BatchRequestCounts(completed=5, failed=0, total=5))
"""

# 4) Get the output file with the answers to the batch OpenAI queries - one response on each line
# NameError: name 'batch_retrieve_response' is not defined
batch_get_output_response = client.files.content("file-lXt6EchiDEH56Wto7nmi7kPR")
print(batch_get_output_response.text) # <openai._legacy_response.HttpxBinaryResponseContent object at 0x71d1dcfe3ac0>
print("---------------------------------")

jsonl_list = batch_get_output_response.text.splitlines()
print(len(jsonl_list))
jsonl_dict_list = []
x = 1
for jsonl_line in jsonl_list:
    jsonl_dict_line = eval(jsonl_line.replace("null", "''"))
    
    this_output_line = dict()
    this_output_line["custom_id"] = jsonl_dict_line["custom_id"]
    
    #this_output_line["created"] = jsonl_dict_line["response"]["body"]["created"] # doesn't seem to mean much
    
    this_output_line["content"] = jsonl_dict_line["response"]["body"]["choices"][0]["message"]["content"]
    this_output_line["finish_reason"] = jsonl_dict_line["response"]["body"]["choices"][0]["finish_reason"]
    this_output_line["usage"] = jsonl_dict_line["response"]["body"]["usage"]
    this_output_line["error"] =  jsonl_dict_line["error"]
    jsonl_dict_list.append(this_output_line)

df_jsonl = pd.DataFrame(jsonl_dict_list)
#print(df_jsonl)

save_dataframe_to_csv(df_jsonl, "df_100")


exit()
batch_output_path = Path(f"my_image_batch_output100.jsonl")
print(f"WRITING: {batch_output_path}")
with open(batch_output_path, "w") as f:
    f.write(batch_get_output_response.text)































