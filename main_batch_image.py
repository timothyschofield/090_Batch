"""
File : main_batch_image.py

Author: Tim Schofield
Date: 22 July 2024

"""
import openai
from openai import OpenAI
from dotenv import load_dotenv
from helper_functions_batch import get_file_timestamp, is_json, make_payload, clean_up_ocr_output_json_content, are_keys_valid, get_headers, save_dataframe_to_csv

import requests
import os
from pathlib import Path 
import numpy as np
import pandas as pd
import time
from datetime import datetime
import json
import sys

print(f"Python version {sys.version}")

def create_jsonl_batch_line(request_id, model, prompt, url_request, num_tokens):
    
    messages = f'[{{"role": "user","content": [{{"type": "text", "text": "{prompt}"}}, {{"type": "image_url", "image_url": {{"url": "{url_request}"}}}}]}}]'

    ret =   (
            f'{{"custom_id": "{request_id}", "method": "POST", "url": "/v1/chat/completions", "body": {{"model": "{model}", "messages": {messages}, "max_tokens": {num_tokens}}}}}'
            )

    return ret

prompt = (f"Read this herbarium sheet and return the text.")
max_tokens = 8192
MODEL = "gpt-4o-mini"


load_dotenv()
try:
    my_api_key = os.environ['OPENAI_API_KEY']          
    client = OpenAI(api_key=my_api_key)
except Exception as ex:
    print("Exception:", ex)
    exit()

input_folder = "ny_herbarium_input"
input_file = "NY_specimens_to_transcribe.csv"
input_path = Path(f"{input_folder}/{input_file}")

input_jpg_folder = "jpg_folder_input"

df_input_csv = pd.read_csv(input_path)

# This would all be fine except that a DarImageURL column can contain multiple image urls in one line seperated a pipes ("|")
# So its easiest just to get it out of the way and make a df copy with each url having its own line - the new df will have more lines obviously
to_transcribe_list = []
for index, row in df_input_csv.iterrows():

    dar_image_url = row["DarImageURL"]
    if "|" in dar_image_url:
        
        url_list = dar_image_url.split("|")
        for url in url_list:
            url = url.strip()
            this_row = df_input_csv.loc[index].copy()
            this_row["DarImageURL"] = url
            to_transcribe_list.append(this_row)
    else:
        this_row  = df_input_csv.loc[index].copy() 
        to_transcribe_list.append(this_row)

df_to_transcribe = pd.DataFrame(to_transcribe_list).fillna('none')
df_to_transcribe["ERROR"] = "none"
df_to_transcribe["MyOcrText"] = "No OCR text"

# Necessary because by copying rows to give each url a seperate row, we have also copied indexes
# We want each row to have its own index - so reset_index
df_to_transcribe.reset_index(drop=True, inplace=True)

headers = get_headers(my_api_key)

prompt = (f"Read this herbarium sheet and return the text.")

jsonl_file_content = f""
source_type="url" # "url" or "local"
print("####################################### START OUTPUT ######################################")
for index, row in df_to_transcribe.iloc[0:100].iterrows():  

    image_path = row["DarImageURL"]
    
    if source_type != "url":
        # JPGs in local folder
        filename = image_path.split("/")[-1]
        image_path = Path(f"{input_jpg_folder}/{filename}")
        if image_path.is_file() == False:
            print(f"File {image_path} does not exist")
            exit()

    print(f"\n########################## OCR OUTPUT {image_path} ##########################")
    print(f"index: {index}")

    #payload = make_payload(model=MODEL, prompt=prompt, source_type=source_type, image_path=image_path, num_tokens=4096)
    #ocr_output = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
    #print(ocr_output.json()['choices'][0]['message']['content'])

    jsonl_line = create_jsonl_batch_line(f"request-{index}", MODEL, prompt, image_path, max_tokens)
    #print(jsonl_line)
    print("---------------------")
    jsonl_file_content = f"{jsonl_file_content}{jsonl_line}\n"


print(f"****{jsonl_file_content}****")
batch_output_path = Path(f"my_image_batch100.jsonl")
print(f"WRITING: {batch_output_path}")
with open(batch_output_path, "w") as f:
    f.write(jsonl_file_content)


