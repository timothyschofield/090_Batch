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
        self.unique_id_mode = "auto"
        
        self.prompt = (f"Read this herbarium sheet and return the text.")
        self.max_tokens = 8192
        self.model = "gpt-4o-mini"
 
    """
    Creates a new JSONL file for batch upload from a CSV
    This is specificaly for doing image OCR - it is not generalised
    The csv should be in the batch_source_csv folder
    The resultant JSONL file will be created in the batch_input folder
    
    num_images_to_batch: must be smaller or equal to the number of lines in the source CSV
        pass in -1 if you want to batch the whole of the souce CSV
        
    unique_id_col: A unique id for the image line. This usualy comes from
    
    
    """
    def create_source_image_jsonl(self, source_csv_file_name, image_col, batch_name, unique_id_col, num_images_to_batch):
        
        self.source_csv_file_name = Path(source_csv_file_name)
        self.source_csv_file_path = Path(f"{self.batch_source_csv_folder}/{self.source_csv_file_name}")
        self.source_csv_image_col = image_col 
        self.batch_name = batch_name
        self.unique_id_col = unique_id_col    
        self.num_images_to_batch = num_images_to_batch 
        
        path_exists(self.source_csv_file_path)
        
        df_input_csv = pd.read_csv(self.source_csv_file_path)
        
        if self.source_csv_image_col in df_input_csv.columns:
            print(f"OK: image column {self.source_csv_image_col} exists.")
        else:
            print(f"ERROR: image column {self.source_csv_image_col} does NOT exists.")
            exit()
        
        len_input_csv = len(df_input_csv)
        if self.num_images_to_batch > len_input_csv:
            print(f"ERROR: The requested number of images to batch is {self.num_images_to_batch} which is longer than the source CSV's {len_input_csv}.")
            exit()
        else:
            if self.num_images_to_batch == -1:
                self.num_images_to_batch = len_input_csv
                
        print(f"OK: number of images to batch is {self.num_images_to_batch}.")
        
        if self.unique_id_col == self.batch_name:
            # There was no unique column to identify each line by so auto mode
            # name the lines batch_name-0, batch_name-1, batch_name-2, etc.
            self.unique_id_mode = "auto"
            print(f"OK: unique_id_mode = auto. Lines in batch will be uniquely identified as {self.batch_name}-0, {self.batch_name}-1, etc.")
        else:
            self.unique_id_mode = "id_col"
            if self.unique_id_col in df_input_csv.columns:
                print(f"OK: unique_id_mode = id_col. Lines in batch will be uniquely identified as according to the {self.unique_id_col} column.")
            else:
                print(f"ERROR: unique id column {self.unique_id_col} does not exist in the source CSV.")
                exit()
        