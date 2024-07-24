"""
    AppNew.py
    Author: Tim Schofield
    Date: 23 July 2024 
"""
from pathlib import Path 
import pandas as pd
import os
from helper_functions_batch import get_file_timestamp,are_keys_valid, get_headers, save_dataframe_to_csv

from BatchController import BatchController

class AppNew:
    def __init__(self, 
                 source_csv_file, 
                 batch_source_csv_folder, 
                 batch_input_folder, 
                 batch_output_folder, 
                 source_csv_image_col, 
                 source_csv_unique_id_col, 
                 model, 
                 prompt, 
                 max_tokens):

        self.paths = dict()
        
        self.paths["source_csv_file"] = self._path_exists(Path(f"{batch_source_csv_folder}/{source_csv_file}"))
        
        source_jsonl_file = f"{Path(source_csv_file).stem}.jsonl"
        self.paths["source_jsonl_file"] = Path(f"{batch_source_csv_folder}/{source_jsonl_file}")
        
        self.paths["input_folder"] = self._path_exists(Path(batch_input_folder))
        self.paths["output_folder"] = self._path_exists(Path(batch_output_folder))
        
        self.batch_controller = BatchController(paths=self.paths, 
                                                source_csv_image_col=source_csv_image_col, 
                                                source_csv_unique_id_col=source_csv_unique_id_col, 
                                                model=model, 
                                                prompt=prompt, 
                                                max_tokens=max_tokens)
        
    """
    """
    def add_batch(self, batch_name):
        self.batch_controller.add_batch(batch_name)
        
    """
    """        
    def upload_batch(self, batch_name):
        self.batch_controller.upload_batch(batch_name)
        
    """
    """         
    def start_batch(self, batch_name):
        self.batch_controller.start_batch(batch_name)
            
    """
    """    
    def get_batch_status(self, batch_name):
        self.batch_controller.get_batch_status(batch_name)
        
    """
    """       
    def download_batch(self, batch_name):
        self.batch_controller.download_batch(batch_name)   
        
    """
    """    
    def delete_batch(self, batch_name):
        self.batch_controller.delete_batch(batch_name)
       

    """
    """
    def _path_exists(self, input_path):
        if os.path.exists(input_path) != True:
            print(f"ERROR: {input_path} file does not exits")
            exit()
        else:
            print(f"OK: READING {input_path}")
            return input_path