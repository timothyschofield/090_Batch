"""
    BatchController.py
    Author: Tim Schofield
    Date: 24 July 2024 

"""

from pathlib import Path 
import pandas as pd
import os
from helper_functions_batch import get_file_timestamp,are_keys_valid, get_headers, save_dataframe_to_csv

from Batch import Batch

class BatchController:
    def __init__(self, paths, source_csv_image_col, source_csv_unique_id_col, model, prompt, max_tokens):
        
        self.paths = paths
        self.batch_list = dict()
        
        self.source_csv_image_col = source_csv_image_col
        self.source_csv_unique_id_col = source_csv_unique_id_col

        self.model = model
        self.prompt = prompt
        self.max_tokens = max_tokens       
        
        self.batch_file_path = "test_name" # <<<<
        
        self.unique_id_mode = None
        
        # It is the BatchController's job to load the data because this is solely to do with Batch creating
        self.df_input_csv = pd.read_csv(self.paths["source_csv_file"])
        
        if self.source_csv_image_col in self.df_input_csv.columns:
            print(f"OK: image column {self.source_csv_image_col} exists.")
        else:
            print(f"ERROR: image column {self.source_csv_image_col} does NOT exists.")
            exit()   
                 
        """
            source_csv_unique_id_col: The name of the column in the source CSV that is unique id for that line.
            If you don't have a unique id column to pass into source_csv_unique_id_col then 
            pass in any string that is NOT the name of a column in the source CSV.
        """
        if self.source_csv_unique_id_col in self.df_input_csv.columns:
            self.unique_id_mode = "unique_id_col"
            print(f"OK: unique_id_mode = unique_id_col. Lines in the batch will be uniquely identified according to the {self.source_csv_unique_id_col} column.")
        else:
            self.unique_id_mode = "auto"
            print(f"OK: unique_id_mode = auto. Lines in batch will be uniquely identified as {self.source_csv_unique_id_col}-0, {self.source_csv_unique_id_col}-1, etc.")        
        
        jsonl_file_content = f""
        for index, row in self.df_input_csv[0:].iterrows():
            
            if self.unique_id_mode == "auto":
                custom_id = f"{self.source_csv_unique_id_col}-{index}"
            else:
                custom_id = row[self.source_csv_unique_id_col]
            
            jsonl_line = self._create_jsonl_batch_line(custom_id, self.model, self.prompt, row[self.source_csv_image_col], self.max_tokens)
            jsonl_file_content = f"{jsonl_file_content}{jsonl_line}\n"
            
        print(f"WRITING: {self.paths['source_jsonl_file']}")
        with open(self.paths["source_jsonl_file"], "w") as f:
            f.write(jsonl_file_content)       
        
    """
        Add a Batch to the BatchController
    """    
    def add_batch(self, batch_name):
        self.batch_list[batch_name] = Batch(batch_name)
        
    """
        Process the from_line -> to_line of CSV and create a JSONL Batch file
    """
    def create_batch_file(self, batch_name):
        pass
        
    """
        Upload the JSONL Batch file to OpenAI Batch processing
    """    
    def upload_batch(self, batch_name):
        self.batch_list[batch_name].upload()
        
    """
        Tell OpenAI to start processing the Batch
    """    
    def start_batch(self, batch_name):
        self.batch_list[batch_name].start()
        
    """
        Get the Batch status - including if it is complete
    """    
    def get_batch_status(self, batch_name):
        self.batch_list[batch_name].get_status()
        
    """
        Get the JSONL data resulting from a completed Batch
    """   
    def download_batch(self, batch_name):
        self.batchLbatch_listist[batch_name].download()
         
    """
        Delete a Batch from OpenAI
    """    
    def delete_batch(self, batch_name):
        self.batch_list[batch_name].delete() 
        
    """
    """      
    def _create_jsonl_batch_line(self, custom_id, model, prompt, url_request, max_tokens):
    
        messages = f'[{{"role": "user","content": [{{"type": "text", "text": "{prompt}"}}, {{"type": "image_url", "image_url": {{"url": "{url_request}"}}}}]}}]'

        ret = f'{{"custom_id": "{custom_id}", "method": "POST", "url": "/v1/chat/completions", "body": {{"model": "{model}", "messages": {messages}, "max_tokens": {max_tokens}}}}}'

        return ret       
   
    
    