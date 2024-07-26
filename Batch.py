"""
    Batch.py
    Author: Tim Schofield
    Date: 24 July 2024 
"""
from pathlib import Path 
import pandas as pd
import os
from helper_functions_batch import get_file_timestamp, save_dataframe_to_csv, path_exists


class Batch():
    def __init__(self, 
                 openai_client, 
                 input_folder, 
                 output_folder, 
                 batch_name, 
                 source_csv_path,
                 from_line,
                 to_line,
                 source_csv_image_col, 
                 source_csv_unique_id_col, 
                 model, 
                 prompt, 
                 max_tokens,
                 endpoint):
        
        self.openai_client = openai_client
        self.batch_name = batch_name
       
        self.input_folder = path_exists(Path(input_folder))
        self.input_file_path = Path(f"{self.input_folder}/{self.batch_name}_input.jsonl")
        
        self.output_folder = path_exists(Path(output_folder)) 
        self.output_file_path = Path(f"{self.output_folder}/{self.batch_name}_output.jsonl")
        
        self.source_csv_path = path_exists(Path(source_csv_path))
        self.from_line = from_line
        self.to_line = to_line
        self.source_csv_image_col = source_csv_image_col
        self.source_csv_unique_id_col = source_csv_unique_id_col
        
        self.model = model
        self.prompt = prompt
        self.max_tokens = max_tokens       
        self.endpoint = endpoint
        
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
        expired	the     batch was not able to be completed within the 24-hour time window
        cancelling	    the batch is being cancelled (may take up to 10 minutes)
        cancelled	    the batch was cancelled
        """      
        self.batch_status = None    

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
            pass in any string that is NOT the name of a column in the source CSV.
        """
        if self.source_csv_unique_id_col in self.df_input_csv.columns:
            self.unique_id_mode = "unique_id_col"
            print(f"OK {self.batch_name}: unique_id_mode = unique_id_col. Lines in the batch will be uniquely identified according to the {self.source_csv_unique_id_col} column.")
        else:
            self.unique_id_mode = "auto"
            print(f"OK {self.batch_name}: unique_id_mode = auto. Lines in batch will be uniquely identified as {self.source_csv_unique_id_col}-0, {self.source_csv_unique_id_col}-1, etc.") 
                
        jsonl_file_content = f""
        for index, row in self.df_input_csv[self.from_line:self.to_line].iterrows():
            
            if self.unique_id_mode == "auto":
                custom_id = f"{self.source_csv_unique_id_col}-{index:05}"
            else:
                custom_id = row[self.source_csv_unique_id_col]
            
            jsonl_line = self._create_jsonl_batch_line(custom_id=custom_id, url_request=row[self.source_csv_image_col], endpoint=self.endpoint)
            jsonl_file_content = f"{jsonl_file_content}{jsonl_line}\n"   
    
        print(f"WRITING {self.batch_name}: {self.input_file_path}")
        with open(self.input_file_path, "w") as f:
            f.write(jsonl_file_content)     
    
    """
    """ 
    def do_batch(self):
        print(f"OK DO BATCH: {self.batch_name}")
        self.upload()
        self.create()
        self.get_status()
    """
    completion_window="24h"
    This is the maximum time the batch is allowed to take
    If it does not complete in theis times, then unprocesses JSONL fails?
    """ 
    def upload(self):
        self.batch_upload_response = self.openai_client.files.create(file=open(self.input_file_path, "rb"), purpose="batch") 
        print(self.batch_upload_response)
        print("----------------------")
    """
    """ 
    def create(self):
        self.batch_info_response = self.openai_client.batches.create(input_file_id=self.batch_upload_response.id, endpoint=self.endpoint, completion_window="24h")
        self.batch_id = self.batch_info_response.id
        self.batch_status = self.batch_info_response.status
        print(self.batch_info_response)        
        print("----------------------")
    """
    """ 
    def get_status(self):
        self.batch_info_response = self.openai_client.batches.retrieve(self.batch_id)
        self.batch_id = self.batch_info_response.id
        self.batch_status = self.batch_info_response.status
        return self.batch_info_response
    """
    """ 
    def download(self):
        pass        
        
    """
    """ 
    def delete(self):
        pass          
        
    """
    """      
    def _create_jsonl_batch_line(self, custom_id, url_request, endpoint):
    
        messages = f'[{{"role": "user","content": [{{"type": "text", "text": "{self.prompt}"}}, {{"type": "image_url", "image_url": {{"url": "{url_request}"}}}}]}}]'

        ret = f'{{"custom_id": "{custom_id}", "method": "POST", "url": "{endpoint}", "body": {{"model": "{self.model}", "messages": {messages}, "max_tokens": {self.max_tokens}}}}}'

        return ret    
        
        
        
        
        
        