"""
    App.py

"""
from pathlib import Path 
import pandas as pd
import os
from helper_functions_batch import get_file_timestamp,are_keys_valid, get_headers, save_dataframe_to_csv

def path_exists(input_path):
    if os.path.exists(input_path) != True:
        print(f"ERROR: {input_path} file does not exits")
        exit()
    else:
        print(f"OK: READING {input_path}")

class App:
    def __init__(self):
    
        self.batch_source_csv_folder =  Path(f"batch_source_csv")
        self.batch_input_folder =  Path(f"batch_input")
        self.batch_output_folder =  Path(f"batch_output")
        
        self.source_csv_file_name = None
        self.source_csv_file_path = None
        self.source_csv_image_col = None
        
        path_exists(self.batch_source_csv_folder)
        path_exists(self.batch_input_folder)
        path_exists(self.batch_output_folder)
        
        self.batch_name = None
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
    The resultant JSONL file will be created in the batch_input folder and have the same name as the source CSV, exept with a JSONl extension.
    This method translates the whole of the CSV into a JSONL file, line by line.
    
    To create a file for batch processing, you sample from this file - and create a second file for uploading.
    For instance you may want to batch process lines 0 to 100 or line 200 to 700. This is a secondary process in the workflow.
    
    unique_id_col: The name of the column in the source CSV containing a unique id for the line.
    If you don't have a unique id column to pass into unique_id_col then pass in "custom_id"
    The JSONL lines will then be uniquely identified as custom_id-0, custom_id-1, custom_id-2, etc.
    
    """
    def create_source_jsonl(self, source_csv_file_name, image_col, unique_id_col):
        
        self.source_csv_file_name = Path(source_csv_file_name)
        self.source_csv_file_path = Path(f"{self.batch_source_csv_folder}/{self.source_csv_file_name}")
        self.source_csv_image_col = image_col
        self.unique_id_col = unique_id_col
        
        path_exists(self.source_csv_file_path)
        
        df_input_csv = pd.read_csv(self.source_csv_file_path)
        
        if self.source_csv_image_col in df_input_csv.columns:
            print(f"OK: image column {self.source_csv_image_col} exists.")
        else:
            print(f"ERROR: image column {self.source_csv_image_col} does NOT exists.")
            exit()
        
        if self.unique_id_col == "custom_id":
            # There was no unique column to identify each line by so auto mode
            # name the lines custom_id-0, custom_id-1, custom_id-2, etc.
            self.unique_id_mode = "auto"
            print(f"OK: unique_id_mode = auto. Lines in batch will be uniquely identified as custom_id-0, custom_id-1, etc.")
        else:
            self.unique_id_mode = "unique_id_col"
            if self.unique_id_col in df_input_csv.columns:
                print(f"OK: unique_id_mode = unique_id_col. Lines in the batch will be uniquely identified as according to the {self.unique_id_col} column.")
            else:
                print(f"ERROR: unique id column {self.unique_id_col} does not exist in the source CSV.")
                exit()
        
        
        
        
        
        
        
        
        
        