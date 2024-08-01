"""
    BatchNew.py
    Author: Tim Schofield
    Date: 31 July 2024 
    
            Batch
            
            Need to do it all in memory - forget about intermediate files
            1. Read inital CSV and store the JSONL_from_CSV as a dict by unique_id column
            2. Create an empty JSONL_returned dict by unique_id column
            
            There is a distinction between a Batch and an API batch
            Batch is always initalised from a CSV
            Then Batch can set off an API batch,
            It comes back with failiers
            Batch sets of another API batch off etc - It doesn't care about previouse API batches - and dosn't need to keep a record of them
            as long as the returned JSONL has been accumulated.
            
            
    subclssses BatchFromCSV and BatchFromJSONL ?
    
"""
from pathlib import Path 
import pandas as pd
import os 
import time

from batch_package import batch_utils

class BatchNew():
    def __init__(self, openai_client, input_folder, output_folder, batch_data):
        
        self.openai_client = openai_client
        self.batch_name = batch_data["batch_name"]
        
        self.input_folder = batch_utils.path_exists(Path(input_folder))
        self.input_file_path = Path(f"{self.input_folder}/{self.batch_name}_input.jsonl")
        self.output_folder = batch_utils.path_exists(Path(output_folder)) 
        self.output_file_path = Path(f"{self.output_folder}/{self.batch_name}_output")

        self.batch_data = batch_data

        # JSONL generated from the input CSV for Batch API upload
        # dict by unique_id CSV column
        self.JSONL_from_CSV = dict()

        # JSONL_returned starts as an empty dict by unique_id CSV column.
        # It accumulates the JSONL returned from the Batch API.
        # If on the first Batch API upload there are no failiers then JSONL_returned will be entirly populated with JSONL and is used to create the JSONL output file.
        # If there are failiers then the failed lines are copied from JSONL_from_CSV for a subsequent upload. This is done iterativley.
        # The results are accumulated in JSONL_returned until there are no failed lines and a JSONL output file can be created.
        # There is a maximum_tries limit so the process is guarenteed to terminate.
        self.JSONL_returned = dict()

        # These are returned from the Batch API
        """
        batch_status    Description
        ---------------------------
        validating	    the input file is being validated before the batch can begin
        failed	        the input file has failed the validation process
        in_progress	    the input file was successfully validated and the batch is currently being run
        finalizing	    the batch has completed and the results are being prepared
        completed	    the batch has been completed and the results are ready
        expired	        the batch was not able to be completed within the 24-hour time window
        cancelling	    the batch is being cancelled (may take up to 10 minutes)
        cancelled	    the batch was cancelled
        """  
        self.batch_id = None                # A hash assigned by the Batch API
        self.batch_status = None            # The important ones are "in_progress" and "completed"
        self.batch_output_file_id = None    # Needed for getting the batch_text_content from the Batch API
        self.batch_text_content = None      # Returned as JSONL from the Batch API    
        self.batch_request_counts = None    # {completed: num_X, failed: num_Y, total: num_Z}

        # "started", "uploaded", "processing", "finished"
        self.batch_control_status = "started"
        self.start_time = None
        
        self.import_csv()        

"""
    Inheritance was from here down
"""
def import_csv(self):
    
    self.source_csv_path = batch_utils.path_exists(Path(self.batch_data["source_csv_path"]))
    self.from_line = self.batch_data["from_line"]
    self.to_line = self.batch_data["to_line"]
    self.source_csv_image_col = self.batch_data["source_csv_image_col"]
    self.source_csv_unique_id_col = self.batch_data["source_csv_unique_id_col"]
    
    self.model = self.batch_data["model"]
    self.prompt = self.batch_data["prompt"]
    self.max_tokens = self.batch_data["max_tokens"]
    self.endpoint = self.batch_data["endpoint"]
    
    self.unique_id_mode = None

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
            
    self.upload_file_content = f""
    for index, row in self.df_input_csv[self.from_line: self.to_line].iterrows():
        
        if self.unique_id_mode == "auto":
            custom_id = f"{self.source_csv_unique_id_col}-{index:05}"
        else:
            custom_id = row[self.source_csv_unique_id_col]
        
        # Yes - should probably be a method
        jsonl_line = batch_utils.create_jsonl_batch_line(custom_id=custom_id, 
                                                            url_request=row[self.source_csv_image_col], 
                                                            endpoint=self.endpoint, 
                                                            model=self.model, 
                                                            prompt=self.prompt, 
                                                            max_tokens=self.max_tokens)
        
        self.upload_file_content = f"{self.upload_file_content}{jsonl_line}\n"  

"""
    Write the for uploading/inputting to the Batch API 
"""
def write_upload_jsonl(self):
    
    print(f"WRITING {self.batch_name}: {self.input_file_path}")
    with open(self.input_file_path, "w") as f:
        f.write(self.upload_file_content)  




















