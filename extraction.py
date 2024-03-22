from utils import *
from aws_utils import *
from tqdm import tqdm

import multiprocessing, requests, warnings
import pandas as pd

warnings.filterwarnings("ignore")


# basic configuration from external file
with open("./config.json") as file:
    config = dict(json.load(file))


# = = = = = PREPARATION = = = = =
# - - - - - Extraction
# single job - extraction routine
def extract_json_from_url(args):
    (job, trys, path_log, path_result_raw, log_terminal_macro, log_terminal_micro) = args

    result_content = None
    atempts = 0
    prefix = f'JOB {job["name"]}, CATEGORY {job["category"]} -'

    while atempts < trys and result_content == None:
        try:
            response = requests.request(method="GET", url=job["url"], verify=False, timeout=1e6)     
            if response.status_code == 200:
                try:
                    result = response.json()
                    if result != "":
                        try:
                            with open(f'{path_result_raw}{job["name"]}_{job["category"]}.json', 'w', encoding='utf-8') as file:
                                json.dump(
                                    result, 
                                    file,
                                    ensure_ascii=False, 
                                    indent=4
                                    )
                            log(f"{prefix} Finished job.", path_log, log_terminal=log_terminal_micro)    
                            break
                        except Exception as e:
                            log(f"{prefix} Error saving JSON file:\n{e}", path_log, log_terminal=log_terminal_macro)
                            if config["stop_if_error"]: raise
                    else:
                        log(f"{prefix} Empty JSON file.", path_log, log_terminal=log_terminal_macro)
                except Exception as e:
                    log(f"{prefix} Error parsing JSON results:\n{e}", path_log, log_terminal=log_terminal_macro)
                    if config["stop_if_error"]: raise
        except Exception as e:
            log(f"{prefix} Error on performing request:\n{e}", path_log, log_terminal=log_terminal_macro)
            if config["stop_if_error"]: raise
        atempts += 1

# interface - multiprocessing extraction
def extract(config, log_terminal_macro=config["terminal_log_macro"], log_terminal_micro=config["terminal_log_micro"]):

    setup = config["raw_source"]

    max_proc = multiprocessing.cpu_count()
    max_proc = config["max_proc"] if config["max_proc"] < max_proc else max_proc
    max_trys = config["max_get_trys"]
    
    path_log = config["local"]["path_log"]
    path_result_raw = config["local"]["path_result_raw"]
    prep_dir([get_dir_from_file_path(path_log), path_result_raw])

    log(f"\n\n", path_log, log_terminal=log_terminal_macro)
    log(f"STARTING ALL EXTRACTION PROCESSES", path_log, log_terminal=log_terminal_macro)
    with multiprocessing.Pool(processes=(max_proc if len(setup) > max_proc else len(setup))) as pool:
        with tqdm(total=len(setup), position=0, leave=True, disable=(not log_terminal_macro)) as global_pbar:
            for _ in pool.imap_unordered(
                extract_json_from_url,
                [(s, max_trys, path_log, path_result_raw, log_terminal_macro, log_terminal_micro) for s in setup],
                chunksize=1
                ):
                global_pbar.update()
    log(f"FINISHED ALL EXTRACTION PROCESSES\n\n", path_log, log_terminal=log_terminal_macro)
# - - - - -


# - - - - - Compression and pre treatent
# single job - file compression (.parquet from .json file)
def compress_to_parquet(job, path_result_raw, path_result_compressed, path_log, log_terminal_macro, log_terminal_micro):
    prefix = f'JOB {job["name"]}, CATEGORY {job["category"]} -'

    # raw df loading
    try:
        log(f"{prefix} Starting loading/reading.", path_log, log_terminal=log_terminal_micro)    
        df = pd.DataFrame.from_dict(read_raw_json(f'./{path_result_raw}{job["name"]}_{job["category"]}.json'))
        log(f"{prefix} Finished loading/reading.", path_log, log_terminal=log_terminal_micro)    
    except Exception as e:
        log(f"{prefix} Error on loading/reading:\n{e}", path_log, log_terminal=log_terminal_macro)
        if config["stop_if_error"]: raise

    # defining multiprocessing chunks, limited according to the size of the df and config
    try:
        chunk_size = 10**int(config["max_df_size_magnitude"])
        chunk_size = df.size if chunk_size > df.size else chunk_size
        chunk_rows = int(chunk_size/df.shape[1])
        chunks = [df.iloc[i:i + chunk_rows] for i in range(0, df.shape[0], chunk_rows)]
    except Exception as e:
        log(f"{prefix} Error :\n{e}", path_log, log_terminal=log_terminal_macro)
        if config["stop_if_error"]: raise
        
    # multiprocessing for numeric value fixes
    log(f"{prefix} Starting compression processes", path_log, log_terminal=log_terminal_macro)
    max_proc = multiprocessing.cpu_count()
    max_proc = config["max_proc"] if config["max_proc"] < max_proc else max_proc
    with multiprocessing.Pool(processes = (max_proc if len(chunks) > max_proc else len(chunks))) as pool:
        results = list(
            tqdm(
                pool.imap_unordered(fix_df_num, chunks, chunksize=1),
                total=len(chunks),
                disable=(not log_terminal_macro)
                )
            )
    results = pd.concat(results)
    log(f"{prefix} Finished compression processes\n", path_log, log_terminal=log_terminal_micro)
    
    # df sorting and saving
    try:
        results.sort_index().to_parquet(f'{path_result_compressed}{job["name"]}_{job["category"]}.parquet')
        log(f"{prefix} Finished saving.", path_log, log_terminal=log_terminal_micro)    
    except Exception as e:
        log(f"{prefix} Error on saving:\n{e}", path_log, log_terminal=log_terminal_macro)
        if config["stop_if_error"]: raise

# interface - batch file compression
def compress(config, log_terminal_macro=config["terminal_log_macro"], log_terminal_micro=config["terminal_log_micro"]):
    path_log = config["local"]["path_log"]
    path_result_raw = config["local"]["path_result_raw"]
    path_result_compressed = config["local"]["path_result_compressed"]

    if os.path.exists(path_result_raw) and (len(os.listdir(path_result_raw)) > 0):
        prep_dir([get_dir_from_file_path(path_log), path_result_compressed])
        
        raw_available = [
            job 
            for job in config["raw_source"] 
            if f'{job["name"]}_{job["category"]}.json' in os.listdir(path_result_raw)
            ]

        log(f"\n\n", path_log, log_terminal=log_terminal_macro)
        log(f"STARTING COMPRESSION AND PRE-TREATMENT PROCEDURE", path_log, log_terminal=log_terminal_macro)
        for job in raw_available:
            prefix = f'JOB {job["name"]}, CATEGORY {job["category"]} -'
            try:
                compress_to_parquet(job, path_result_raw, path_result_compressed, path_log, log_terminal_macro, log_terminal_micro)
                log(f"{prefix} Finished file compression.\n", path_log, log_terminal=log_terminal_micro)    
            except Exception as e:
                log(f"{prefix} Error compressing file:\n{e}", path_log, log_terminal=log_terminal_macro)
                if config["stop_if_error"]: raise
        log(f"FINISHED COMPRESSION PRE-TREATMENT PROCEDURE\n\n", path_log, log_terminal=log_terminal_macro)
    else:
        log(f"Raw file directory is empty or does not exist.", path_log, log_terminal=log_terminal_macro)
# - - - - -


# - - - - - File upload interface with S3
def send_batch_to_s3(config, batch_directory, s3_directory_uri, log_terminal_macro=config["terminal_log_macro"], log_terminal_micro=config["terminal_log_micro"]):

    path_log = config["local"]["path_log"]
    
    # matches files and jobs
    file_job_list = [
        (f, job)
        for job in config["raw_source"]
        for f in os.listdir(batch_directory)
        if f.split(".")[0] == f'{job["name"]}_{job["category"]}'
    ]

    log(f"\n\n", path_log, log_terminal=log_terminal_macro)
    log(f"STARTING AWS S3 UPLOAD PROCESSES FOR {batch_directory}", path_log, log_terminal=log_terminal_macro)
    for (f, job) in tqdm(file_job_list, total=len(file_job_list), disable=(not log_terminal_macro)):
        prefix = f'JOB {job["name"]}, CATEGORY {job["category"]} -'
        try:
            send_to_s3(
                source_file_path=f'{batch_directory}{f}',
                directory_uri=s3_directory_uri,
                termianl_progress=log_terminal_micro)
            log(f"{prefix} Finished sending to S3.", path_log, log_terminal=log_terminal_micro)
        except:
            log(f"{prefix} Error sending to S3.", path_log, log_terminal=log_terminal_macro)
    log(f"FINISHED UPLOAD PROCESSES FOR {batch_directory}\n\n", path_log, log_terminal=log_terminal_macro)
# - - - - -


# - - - - - Procedure call via config file
def perform(step):
    match step:
        case "extraction": 
            extract(config)
        case "compression":
            compress(config)
        case "send_raw_to_s3":
            send_batch_to_s3(
                config,
                config["local"]["path_result_raw"],
                config["AWS"]["destination_raw"],
                )
        case "send_compressed_to_s3":
            send_batch_to_s3(
                config,
                config["local"]["path_result_compressed"],
                config["AWS"]["destinaiton_compressed"],
                )
# - - - - -


# = = = = = EXECUTION = = = = =
for (step, to_perform) in config["steps_to_perform"].items():
    log(f"STARTING ALL PROCESSES\n\n", config["local"]["path_log"], log_terminal=config["terminal_log_macro"])
    if to_perform: perform(step) # only works on ordered dictionaries (python >= 3.7)
    log(f"FINISHED ALL PROCESSES\n\n", config["local"]["path_log"], log_terminal=config["terminal_log_macro"])
    