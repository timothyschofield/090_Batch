"""
    Batch.py
    Author: Tim Schofield
    Date: 24 July 2024 

    Batch does not inherit from BatchController, 
    they has a parts-of relationship

"""

from pathlib import Path 
import pandas as pd
import os
from helper_functions_batch import get_file_timestamp,are_keys_valid, get_headers, save_dataframe_to_csv, path_exists


class Batch():
    def __init__(self, input_folder, output_folder, batch_name, source_csv_path, source_csv_image_col, source_csv_unique_id_col, model, prompt, max_tokens):
        
        self.batch_name = batch_name
       
        self.input_folder = path_exists(Path(input_folder))
        self.input_file_path = Path(f"{self.input_folder}/{self.batch_name}_input.jsonl")
        
        self.output_folder = path_exists(Path(output_folder)) 
        self.output_file_path = Path(f"{self.output_folder}/{self.batch_name}_output.jsonl")
        
        self.source_csv_path = path_exists(Path(source_csv_path))
        self.source_csv_image_col = source_csv_image_col
        self.source_csv_unique_id_col = source_csv_unique_id_col
        
        self.model = model
        self.prompt = prompt
        self.max_tokens = max_tokens       
       
        self.unique_id_mode = None
       
        self.df_input_csv = pd.read_csv(self.source_csv_path)
    
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
        for index, row in self.df_input_csv[0:].iterrows():
            
            if self.unique_id_mode == "auto":
                custom_id = f"{self.source_csv_unique_id_col}-{index}"
            else:
                custom_id = row[self.source_csv_unique_id_col]
            
            jsonl_line = self._create_jsonl_batch_line(custom_id, row[self.source_csv_image_col])
            jsonl_file_content = f"{jsonl_file_content}{jsonl_line}\n"   
    
        print(f"WRITING {self.batch_name}: {self.input_file_path}")
        with open(self.input_file_path, "w") as f:
            f.write(jsonl_file_content)     
    
    """
    """ 
    def do_batch(self):
        print(f"OK {self.batch_name}: DO BATCH")
        pass 
    
    """
    """ 
    def upload(self):
        pass 
      
    """
    """ 
    def start(self):
        pass        
        
    """
    """ 
    def get_status(self):
        pass         
        
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
    def _create_jsonl_batch_line(self, custom_id, url_request):
    
        messages = f'[{{"role": "user","content": [{{"type": "text", "text": "{self.prompt}"}}, {{"type": "image_url", "image_url": {{"url": "{url_request}"}}}}]}}]'

        ret = f'{{"custom_id": "{custom_id}", "method": "POST", "url": "/v1/chat/completions", "body": {{"model": "{self.model}", "messages": {messages}, "max_tokens": {self.max_tokens}}}}}'

        return ret    
        
        
        
        
        
        