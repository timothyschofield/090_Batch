"""
    App.py

"""
from pathlib import Path 
import pandas as pd
import os
from helper_functions_batch import get_file_timestamp,are_keys_valid, get_headers, save_dataframe_to_csv

class App:
    def __init__(self):
    
        self.batch_source_csv_folder =  Path(f"batch_source_csv")
        self.batch_input_folder =  Path(f"batch_input")
        self.batch_output_folder =  Path(f"batch_output")
        
        self.source_csv_file_name = None
        self.source_csv_file_path = None
        self.source_csv_image_col = None
        
        self._path_exists(self.batch_source_csv_folder)
        self._path_exists(self.batch_input_folder)
        self._path_exists(self.batch_output_folder)
        
        self.batch_file_name = None
        self.batch_file_file_path = None   
        
        self.unique_id_col = None
        self.unique_id_mode = None # "auto" or "unique_id_col"
        
        # This code is not fully generalised and only deals with OCRing images
        self.prompt = (f"Read this herbarium sheet and return the text.")
        self.max_tokens = 8192
        self.model = "gpt-4o-mini"
 
    """
    create_source_jsonl(source_csv_file_name, image_col, unique_id_col)
    
    Creates a JSONL file from a CSV
    
    This is specificaly for doing image OCR - it is not generalised
    The source CSV should be in the batch_source_csv folder
    The resultant JSONL file will be created in the batch_source_csv folder and have the same name as the source CSV, exept with a JSONl extension.
    This method translates the whole of the CSV into a JSONL file, line by line.
    
    To create a file for batch processing, you sample from this JSONL file - and create a second JSONL file for uploading.
    For instance you may want to batch process lines 0 to 100 or lines 200 to 700. This is a secondary process in the workflow.
    
    unique_id_col: The name of the column in the source CSV containing a unique id for the line.
    If you don't have a unique id column to pass into unique_id_col then 
    pass in any string that is NOT the name of a column in the source CSV
    """
    def create_source_jsonl(self, source_csv_file_name, batch_file_name, image_col, unique_id_col, from_line, to_line):
        
        self.source_csv_file_name = Path(source_csv_file_name)
        self.source_csv_file_path = Path(f"{self.batch_source_csv_folder}/{self.source_csv_file_name}")
        
        self.batch_file_name = Path(f"{batch_file_name}.jsonl")
        self.batch_file_path = Path(f"{self.batch_input_folder}/{self.batch_file_name}")
        
        self.source_csv_image_col = image_col
        self.unique_id_col = unique_id_col
  
        self._path_exists(self.source_csv_file_path)
        
        df_input_csv = pd.read_csv(self.source_csv_file_path)
        
        if self.source_csv_image_col in df_input_csv.columns:
            print(f"OK: image column {self.source_csv_image_col} exists.")
        else:
            print(f"ERROR: image column {self.source_csv_image_col} does NOT exists.")
            exit()
        
        if self.unique_id_col in df_input_csv.columns:
            self.unique_id_mode = "unique_id_col"
            print(f"OK: unique_id_mode = unique_id_col. Lines in the batch will be uniquely identified as according to the {self.unique_id_col} column.")
        else:
            self.unique_id_mode = "auto"
            print(f"OK: unique_id_mode = auto. Lines in batch will be uniquely identified as {self.unique_id_col}-0, {self.unique_id_col}-1, etc.")
        
        jsonl_file_content = f""
        for index, row in df_input_csv[0:].iterrows():
            
            if self.unique_id_mode == "auto":
                custom_id = f"{self.unique_id_col}-{index}"
            else:
                custom_id = row[self.unique_id_col]
            
            jsonl_line = self._create_jsonl_batch_line(custom_id, self.model, self.model, row[self.source_csv_image_col], self.max_tokens)
            jsonl_file_content = f"{jsonl_file_content}{jsonl_line}\n"
            
        print(f"WRITING: {self.batch_file_path}")
        with open(self.batch_file_path, "w") as f:
            f.write(jsonl_file_content)
       
       
       
       
       
       
       
       
            
    """
    """      
    def _create_jsonl_batch_line(self, request_id, model, prompt, url_request, max_tokens):
    
        messages = f'[{{"role": "user","content": [{{"type": "text", "text": "{prompt}"}}, {{"type": "image_url", "image_url": {{"url": "{url_request}"}}}}]}}]'

        ret = f'{{"custom_id": "{request_id}", "method": "POST", "url": "/v1/chat/completions", "body": {{"model": "{model}", "messages": {messages}, "max_tokens": {max_tokens}}}}}'

        return ret
    
    """
    """
    def _path_exists(self, input_path):
        if os.path.exists(input_path) != True:
            print(f"ERROR: {input_path} file does not exits")
            exit()
        else:
            print(f"OK: READING {input_path}")
        