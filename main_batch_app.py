"""
    File: main_batch_app.py
    Author: Tim Schofield
    Date: 23 July 2024  

    This is the main entry point to the image batch application.
    This uses the OpenAI Batch processing API
    
    It is a wrapper around some of the methods demonstrated in main_batch_basics.py and main_batch_image.py - but not dependant on them.
    
    This App takes a source CSV with a column of URLs, generates a JSONL input file from the CSV and uploades it for batch processing.
    The batch is then processes - it is OCRed by OpenAI API with the prompt and model provided.

    During processing the batch status is polled every few seconds or minutes and when it's status changes to "completed"
    the resultant data is downloaded and saved in it's entirety as a JSONL output file.
    A CSV file of certain useful values in the data is also created - such as the OCR output and unique identifier.

"""
from App import App

app1 = App()


batch_name1 = "test_batch_10linesTIM"
batch_name2 = "test_batch_10linesSIMON"

source_csv_path = "batch_source_csv/NY_specimens_to_transcribe_test.csv"
source_csv_image_col = "DarImageURL"

"""
    source_csv_unique_id_col: The name of the column in the source CSV that is unique id for that line.
    If you don't have a unique id column to pass into source_csv_unique_id_col then 
    pass in any string that is NOT the name of a column in the source CSV.
"""
source_csv_unique_id_col = "DarCatalogNumber"

model = "gpt-4o-mini"
prompt = f"Read this herbarium sheet and return the text."
max_tokens = 8192
endpoint = "/v1/chat/completions"

app1.do_batch(batch_name1, source_csv_path, source_csv_image_col, source_csv_unique_id_col, model, prompt, max_tokens, endpoint)


app1.do_batch(batch_name2, source_csv_path, source_csv_image_col, source_csv_unique_id_col, model, prompt, max_tokens, endpoint)



