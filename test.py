"""

Starting with a 45 line NY_specimens_to_transcribe_small.csv
test_batch_all_45_lines_output.jsonl had 9 failures and is only 36 lines long

Write program which starts with these two files and
1) identifies which lines are missing
2) makes a NY_specimens_to_transcribe_small_fix_01.csv
    with just the missing lines in      

"""
from pathlib import Path 
import pandas as pd
from batch_package import batch_utils

endpoint = "/v1/chat/completions"
model = "gpt-4o"
max_tokens = 4096

batch_name = "test_batch_all_45_lines"

# This is the origonal, complete, JSONL input file
input_folder = Path("batch_input") 
complete_file = Path(f"{batch_name}_input.jsonl")
complete_file_path = batch_utils.path_exists(Path(f"{input_folder}/{complete_file}"))

with open(complete_file_path, "r") as f:
    complete_jsonl_text = f.read()
complete_jsonl_list = complete_jsonl_text.splitlines()

# This is the file that has come back from Batch processing possibly with missing lines
output_folder = Path("batch_output")
incomplete_file = Path(f"{batch_name}_output.jsonl") 
incomplete_file_path = batch_utils.path_exists(Path(f"{output_folder}/{incomplete_file}"))

with open(incomplete_file_path, "r") as f:
    incomplete_jsonl_text = f.read()
incomplete_jsonl_list = incomplete_jsonl_text.splitlines()

numlines_complete = len(complete_jsonl_list)
numlines_incomplete = len(incomplete_jsonl_list)

print(f"\nNumlines complete {complete_file_path} {numlines_complete}")
print(f"Numlines incomplete {incomplete_file_path} {numlines_incomplete}")

if numlines_complete == numlines_incomplete:
    print(f"The output file: {incomplete_file_path} has the same number of lines as {complete_file_path} - so no fixing to do.")
else:
    print(f"The output file: {incomplete_file_path} has fewer lines than {complete_file_path}\nSo we need to do a FIXUP batch")

"""

jsonl_custom_id_list = []
for jsonl_line in jsonl_list:
    jsonl_dict_line = eval(jsonl_line.replace("null", "''"))
    jsonl_custom_id_list.append(jsonl_dict_line["custom_id"])
   
   
   
   
   
# Now go through the CSV and see which lines are NOT in df_jsonl
# These are the lines that failed in the Batch
jsonl_file_content = f""
for index, row in df_source_csv_path.iloc[0:].iterrows():
    
    # It is best that we compare as strings - because sometimes unique_ids will be strings
    this_unique_id = str(row[source_csv_unique_id_col])
    
    if this_unique_id not in jsonl_custom_id_list:
        # These are the rows that failed in the batch
        print(f"{this_unique_id} NOT in jsonl_custom_id_list")
        
        # Make a JSONL file with the failed lines - to try again
        jsonl_line = batch_utils.create_jsonl_batch_line(custom_id=this_unique_id, url_request=row[source_csv_image_col], endpoint=endpoint, model=model, max_tokens=max_tokens)
        jsonl_file_content = f"{jsonl_file_content}{jsonl_line}\n"   

if jsonl_file_content == f"":
    print(f"All lines present - no fix file created")
else:
    
# Hum?
# test_batch_all_45_lines_output.jsonl
# test_batch_all_45_lines_input_fix1.jsonl
incomplete_file

# Path(image_name).stem

print(f"WRITING: {input_file_path}")
with open(input_file_path, "w") as f:
    f.write(jsonl_file_content) 

"""

