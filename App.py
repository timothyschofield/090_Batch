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
        
        
        self.prompt = (f"Read this herbarium sheet and return the text.")
        self.max_tokens = 8192
        self.model = "gpt-4o-mini"
 
    """
    Creates a new JSONL file for batch upload from a CSV
    This is specificaly for doing image OCR - it is not generalised
    The csv should be in the batch_source_csv folder
    The resultant JSONL file will be created in the batch_input folder
    
    num_images_to_batch sould be smaller or equal to the number of lines in the souce CSV
    pass in -1 if you want to batch the whole of the souce CSV
    """
    def create_source_image_jsonl(self, source_csv_file_name, image_col, num_images_to_batch):
        
        self.source_csv_file_name = Path(source_csv_file_name)
        self.source_csv_file_path = Path(f"{self.batch_source_csv_folder}/{self.source_csv_file_name}")
        self.source_csv_image_col = image_col 
        self.num_images_to_batch = num_images_to_batch 
        
        path_exists(self.source_csv_file_path)
        
        df_input_csv = pd.read_csv(self.source_csv_file_path)
        
        if self.source_csv_image_col in df_input_csv.columns:
            print(f"OK: image column {self.source_csv_image_col} exists")
        else:
            print(f"ERROR: image column {self.source_csv_image_col} does NOT exists")
            exit()
        
        len_input_csv = len(df_input_csv)
        if self.num_images_to_batch > len_input_csv:
            print(f"ERROR: The requested number of images to batch is {self.num_images_to_batch} which is longer than the CSV's {len_input_csv}")
            exit()
        else:
            if self.num_images_to_batch == -1:
                self.num_images_to_batch = len_input_csv
                
        print(f"OK: number of images to batch is {self.num_images_to_batch}")
        
        
        
        
        
        
        
        
        