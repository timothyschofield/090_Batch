"""
    File: main_batch_app.py
    Author: Tim Schofield
    Date: 23 July 2024  

    This is the main entry point to the image batch application.
    This uses the OpenAI Batch processing API
    
    This App takes a source CSV with a column of URLs, generates a JSONL input file from the CSV and uploades it for batch processing.
    The batch is then processes - it is OCRed by OpenAI API with the prompt and model provided.

    During batch processing the batch status is polled every few seconds or minutes and when it's status changes to "completed"
    the resultant data is downloaded and saved in it's entirety as a JSONL output file.
    A CSV file of certain useful values in the data is also created - such as the OCR output and unique identifier.

"""

from App import App

app1 = App()

"""
Here is an example of how to produce two batches which process contiguouse lines of the input CSV
If you want to process all lines to the end of a CSV then set to_line = None

# batch_name1 contains line 0 to 9 - i.e. 10 lines
batch_name1 = "test_batch_10linesTIM"
from_line1 = 0
to_line1 = 10

# batch_name2 contains line 10 to 29 i.e. 20 lines
batch_name2 = "test_batch_20linesSIMON"
from_line2 = 10
to_line2 = 30

"""

"""
    source_csv_unique_id_col: The name of the column in the source CSV that is a unique id for that line.
    If you don't have a unique id column to pass into source_csv_unique_id_col then 
    pass in any string that is NOT the name of a column in the source CSV and the columns will be labeled any_string-0, any_string-1, and so on.
"""

batch1_data = {"batch_name": "test_batch_10_lines", 
               "source_csv_path": "batch_source_csv/NY_specimens_to_transcribe_small.csv", 
               "from_line": 0, 
               "to_line": 10, 
               "source_csv_image_col": "DarImageURL", 
               "source_csv_unique_id_col": "DarCatalogNumber", 
               "model": "gpt-4o-mini", 
               "prompt": "Read this herbarium sheet and return the text.", 
               "max_tokens": 8192, 
               "endpoint": "/v1/chat/completions"}

app1.do_batch(batch1_data)

batch2_data = {"batch_name": "test_batch_to_end_lines", 
               "source_csv_path": "batch_source_csv/NY_specimens_to_transcribe_small.csv", 
               "from_line": 10, 
               "to_line": None, 
               "source_csv_image_col": "DarImageURL", 
               "source_csv_unique_id_col": "DarCatalogNumber", 
               "model": "gpt-4o-mini", 
               "prompt": "Read this herbarium sheet and return the text.", 
               "max_tokens": 8192, 
               "endpoint": "/v1/chat/completions"}

app1.do_batch(batch2_data)



