"""
    BatchController.py
    Author: Tim Schofield
    Date: 24 July 2024 

"""
from pathlib import Path  
import pandas as pd
import os
import time, threading
import datetime
from batch_package import Batch
from batch_package import batch_utils

class BatchController:
    def __init__(self, openai_client):
        
        self.openai_client = openai_client
        
        self.input_folder = batch_utils.path_exists(Path(f"batch_input"))
        self.output_folder = batch_utils.path_exists(Path(f"batch_output"))
        
        self.batch_list = dict()
        
        self.check_status_delay = 5 # seconds
        self.start_time = None
    """
    """
    def do_batch(self, batch_data):
        
        batch_name = batch_data["batch_name"]
        if batch_name in self.batch_list.keys():
            print(f"ERROR {batch_name}: Batch name must be unique - {batch_name} already exists.")
            print(f"{batch_name} is not uploaded.")
            return False
        
        this_batch = Batch.Batch(openai_client=self.openai_client, input_folder=self.input_folder, output_folder=self.output_folder, batch_data=batch_data)

        batch_name = batch_data["batch_name"]
        self.batch_list[batch_name] = this_batch       
        
        this_batch.do_batch()

    """
        This must be called after creating all the Batches to monitor the Batches' progress
        and download the results once a batch in completed
    """
    def start_batches(self):
        self.start_time = int(time.time())
        print(f"============ BATCHES STARTED: {time.ctime()} ============")
        print(f"Number of Batches: {len(self.batch_list)}")
        
        self.check_status()
    
    """
        This is what drives the system
        
        Check the status of all the batches every few seconds/minutes
        When a Batch status becomes "completed" the JSONP and CSV results files are downloaded
    """
    def check_status(self):
        print(time.ctime())
        
        processing_count = 0
        for batch_name, batch in self.batch_list.items():
            batch_status, batch_output_file_id, batch_request_counts, fixup_version = batch.api_get_status()
            print(f"name: {batch_name}, version: {fixup_version}, status: {batch_status}, request_counts: {batch_request_counts}")

            if batch.batch_control_status == "processing":
                processing_count = processing_count + 1
                if batch_status == "completed":
                    batch.batch_control_status = "finished" 
                    batch.finished()
                        
        if processing_count == 0:
            print("============ ALL BATCHES COMPLETED ============")
            end_time = int(time.time())
            print(f"Processing time: {end_time - self.start_time} seconds")
            # exit()
        else: 
            threading.Timer(self.check_status_delay, self.check_status).start()
        
    """
        The batches currently active, completed, processing etc., in the Batch API system
    """
    def display_openai_batches(self):    
        active_batches = self.openai_client.batches.list(limit=100)
        print("---------------------------------")
        print("ACTIVE BATCHES")
        for batch in active_batches.data:
            print(f"id: {batch.id}, status: {batch.status}, created: {datetime.datetime.utcfromtimestamp(batch.created_at)}, {batch.request_counts}, output_file_id: {batch.output_file_id}")
        print("---------------------------------")
       
    """
        If necessary, you can cancel an in_progress batchs. 
        The batch's status will change to cancelling until in-flight requests are complete (up to 10 minutes), 
        after which the status will change to cancelled.
        
        Can't cancel batches with status "failed" or "completed"
        We can only canel batches that are "in_progress"
    """
    def cancel_in_progress_openai_batches(self):
        number_to_cancel = 20
        active_batches = self.openai_client.batches.list(limit=number_to_cancel)
        print(f"CANCELING {number_to_cancel} BATCHES")
        for batch in active_batches.data:
            print(f"id: {batch.id}, status: {batch.status}, created: {datetime.datetime.utcfromtimestamp(batch.created_at)}, {batch.request_counts}, output_file_id: {batch.output_file_id}")
            if batch.status == "in_progress":
                self.openai_client.batches.cancel(batch.id)
            
        print("REMAINING BATCHES")
        self.display_openai_batches()