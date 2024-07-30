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
    A CSV of certain useful values in the data is also created - values like the OCR output and unique identifier.

    Rate Limits

    Batch API rate limits are separate from existing per-model rate limits. 
    
    The Batch API has two new types of rate limits:
    1) Per-batch limits: A single batch may include up to 50,000 requests, and a batch input file can be up to 100 MB in size. 
    Note that /v1/embeddings batches are also restricted to a maximum of 50,000 embedding inputs across all requests in the batch.
    2) Enqueued prompt tokens per model: Each model has a maximum number of enqueued prompt tokens allowed for batch processing. 
    You can find these limits on the Platform Settings page.

    There are no limits for output tokens or number of submitted requests for the Batch API today. 
    Because Batch API rate limits are a new, separate pool, using the Batch API will not consume tokens from your standard per-model rate limits, 
    thereby offering you a convenient way to increase the number of requests and processed tokens you can use when querying our API.
    
    Batch Expiration
    Batches that do not complete in time eventually move to an expired state; unfinished requests within that batch are cancelled, 
    and any responses to completed requests are made available via the batch's output file. 
    You will be charged for tokens consumed from any completed requests.

"""

# python3 __pycache__/App.cpython-310.pyc
from batch_package import App

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

# gpt-4o        max_tokens  4096 equivalent to gpt-4-turbo
# gpt-4o-mini   max_tokens 16348 equivalent to gpt-3.5 - NOT video or audio

batch1_data = {"batch_name": "test_batch_20_lines", 
            "source_csv_path": "batch_source_csv/NY_specimens_to_transcribe_small.csv", 
            "from_line": 0, 
            "to_line": 20, 
            "source_csv_image_col": "DarImageURL", 
            "source_csv_unique_id_col": "DarCatalogNumber", 
            "model": "gpt-4o", 
            "prompt": "Read this herbarium sheet and return the text.", 
            "max_tokens": 4096, 
            "endpoint": "/v1/chat/completions"}

batch2_data = {"batch_name": "test_batch_to_end_lines", 
            "source_csv_path": "batch_source_csv/NY_specimens_to_transcribe_small.csv", 
            "from_line": 10, 
            "to_line": 25, 
            "source_csv_image_col": "DarImageURL", 
            "source_csv_unique_id_col": "DarCatalogNumber", 
            "model": "gpt-4o", 
            "prompt": "Read this herbarium sheet and return the text.", 
            "max_tokens": 4096, 
            "endpoint": "/v1/chat/completions"}

app1.display_openai_batches()

app1.do_batch_from_csv(batch1_data)
app1.do_batch_from_csv(batch2_data)
app1.start_batches()


"""
# All data is already in the JSONL input file
# input file is called test_batch_fix_lines_input_fixup_1.jsonl
# batch_type ?
batch1_data_jsonl = {"batch_name": "test_batch_fix_lines", 
            "source_csv_path": "", 
            "from_line": 0, 
            "to_line": None, 
            "source_csv_image_col": "", 
            "source_csv_unique_id_col": "", 
            "model": "", 
            "prompt": "", 
            "max_tokens": "", 
            "endpoint": ""}

app1.do_batch_from_jsonl(batch1_data_jsonl)
app1.start_batches()
"""