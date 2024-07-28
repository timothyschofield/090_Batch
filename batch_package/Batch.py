"""
    Batch.py
    Author: Tim Schofield
    Date: 24 July 2024 
"""
from pathlib import Path 
import pandas as pd
import os 
import time

from batch_package import batch_utils

class Batch():
    def __init__(self, openai_client, input_folder, output_folder, batch_data):
        
        self.openai_client = openai_client
        self.batch_name = batch_data["batch_name"]
       
        self.input_folder = batch_utils.path_exists(Path(input_folder))
        self.input_file_path = Path(f"{self.input_folder}/{self.batch_name}_input.jsonl")
        
        self.output_folder = batch_utils.path_exists(Path(output_folder)) 
        self.output_file_path = Path(f"{self.output_folder}/{self.batch_name}_output")
        
        self.source_csv_path = batch_utils.path_exists(Path(batch_data["source_csv_path"]))
        self.from_line = batch_data["from_line"]
        self.to_line = batch_data["to_line"]
        self.source_csv_image_col = batch_data["source_csv_image_col"]
        self.source_csv_unique_id_col = batch_data["source_csv_unique_id_col"]
        
        self.model = batch_data["model"]
        self.prompt = batch_data["prompt"]
        self.max_tokens = batch_data["max_tokens"]       
        self.endpoint = batch_data["endpoint"]
        
        self.unique_id_mode = None
       
        # Come from the Batch API when Batch is uploaded or created 
        self.batch_upload_response = None
        self.batch_info_response = None
        self.batch_id = None  
        
        """
        batch_status    Description
        validating	    the input file is being validated before the batch can begin
        failed	        the input file has failed the validation process
        in_progress	    the input file was successfully validated and the batch is currently being run
        finalizing	    the batch has completed and the results are being prepared
        completed	    the batch has been completed and the results are ready
        expired	        the batch was not able to be completed within the 24-hour time window
        cancelling	    the batch is being cancelled (may take up to 10 minutes)
        cancelled	    the batch was cancelled
        """      
        self.api_batch_status = None  
        
        # "started", "uploaded", "processing", "downloaded"
        self.app_batch_status = "started"
        self.start_time = None

        self.df_input_csv = pd.read_csv(self.source_csv_path)
        csv_len = len(self.df_input_csv)
        if self.to_line == None: self.to_line = csv_len
        
        if self.from_line >= self.to_line:
            print(f"ERROR {self.batch_name}: from_line: {self.from_line} must be smaller than to_line: {self.to_line}")
            exit()
            
        if self.to_line > csv_len:
            print(f"ERROR {self.batch_name}: to_line: {self.to_line} must be smaller or equal to than CSV length: {csv_len}")
            exit()
    
    
        if self.source_csv_image_col in self.df_input_csv.columns:
            print(f"OK {self.batch_name}: image column {self.source_csv_image_col} exists.")
        else:
            print(f"ERROR {self.batch_name}: image column {self.source_csv_image_col} does NOT exists.")
            exit()    
    
        """
            source_csv_unique_id_col: The name of the column in the source CSV that is unique id for that line.
            If you don't have a unique id column to pass into source_csv_unique_id_col then 
            pass in any string that is NOT the name of a column in the source CSV and the columns will be labeled any_string-0, any_string-1, and so on.
        """
        if self.source_csv_unique_id_col in self.df_input_csv.columns:
            self.unique_id_mode = "unique_id_col"
            print(f"OK {self.batch_name}: unique_id_mode = unique_id_col. Lines in the batch will be uniquely identified according to the {self.source_csv_unique_id_col} column.")
        else:
            self.unique_id_mode = "auto"
            print(f"OK {self.batch_name}: unique_id_mode = auto. Lines in batch will be uniquely identified as {self.source_csv_unique_id_col}-0, {self.source_csv_unique_id_col}-1, and so on.") 
                
        jsonl_file_content = f""
        for index, row in self.df_input_csv[self.from_line:self.to_line].iterrows():
            
            if self.unique_id_mode == "auto":
                custom_id = f"{self.source_csv_unique_id_col}-{index:05}"
            else:
                custom_id = row[self.source_csv_unique_id_col]
            
            jsonl_line = batch_utils.create_jsonl_batch_line(custom_id=custom_id, url_request=row[self.source_csv_image_col], endpoint=self.endpoint, model=self.model, max_tokens=self.max_tokens)
            jsonl_file_content = f"{jsonl_file_content}{jsonl_line}\n"   
    
        print(f"WRITING {self.batch_name}: {self.input_file_path}")
        with open(self.input_file_path, "w") as f:
            f.write(jsonl_file_content)     
    
    """
    """ 
    def do_batch(self):
        print(f"OK DO BATCH: {self.batch_name}")
        self.start_time = int(time.time())
        self.upload()
        self.create()
        self.get_status()
    """
    completion_window="24h"
    This is the maximum time the batch is allowed to take
    If it does not complete in theis times, then self.api_batch_status = expired
    """ 
    def upload(self):
        self.batch_upload_response = self.openai_client.files.create(file=open(self.input_file_path, "rb"), purpose="batch")
        
        self.api_batch_status = self.batch_upload_response.status
        self.app_batch_status = "uploaded"
        
        #print(self.batch_upload_response)
        #print("----------------------")
    """
    """ 
    def create(self):
        self.batch_info_response = self.openai_client.batches.create(input_file_id=self.batch_upload_response.id, endpoint=self.endpoint, completion_window="24h")
        
        self.batch_id = self.batch_info_response.id
        
        self.api_batch_status = self.batch_info_response.status
        self.app_batch_status = "processing"
        
        #print(self.batch_info_response)        
        #print("----------------------")
    """
    """ 
    def get_status(self):
        self.batch_info_response = self.openai_client.batches.retrieve(self.batch_id)
        
        self.batch_id = self.batch_info_response.id
        self.api_batch_status = self.batch_info_response.status
        
        return self.batch_info_response
    
    """
        Downloads the resultant processed batch as a JSONL file and CSV file
    """ 
    def download(self):
        
        batch_get_content_response = self.openai_client.files.content(self.batch_info_response.output_file_id)        
        jsonl_list = batch_get_content_response.text.splitlines()
        
        jsonl_dict_list = []
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
        
        print(f"WRITING: {self.output_file_path}.csv")
        # Some important details
        batch_utils.save_dataframe_to_csv(df_jsonl, self.output_file_path)
        
        # The returned data in total
        print(f"WRITING: {self.output_file_path}.jsonl")
        with open(f"{self.output_file_path}.jsonl", "w") as f:
            f.write(batch_get_content_response.text)
        
        end_time = int(time.time())
        print(f"Processing time: {end_time - self.start_time} seconds")
        
    """
    """ 
    def cancel(self):
        pass          
        
   
        
    
        
        