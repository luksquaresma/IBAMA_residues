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

# single job - extraction routine
def extract_json_from_url(args):
    
    (job, trys, path_log, path_result_raw, log_terminal) = args

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
                                json.dump(result, file, ensure_ascii=False, indent=4)
                            log(f"{prefix} Finished job.", path_log, log_terminal=log_terminal)    
                            break
                        except:
                            log(f"{prefix} Error saving JSON file.", path_log, log_terminal=log_terminal)    
                    else:
                        log(f"{prefix} Empty JSON file.", path_log, log_terminal=log_terminal)
                except:
                    log(f"{prefix} Error parsing JSON results.", path_log, log_terminal=log_terminal)
        except:
            log(f"{prefix} Error on performing request.", path_log, log_terminal=log_terminal)
        atempts += 1


# interface - multiprocessing extraction
def extract(config, log_terminal=config["terminal_log"]):

    setup = config["raw_source"]

    max_proc = multiprocessing.cpu_count() - 1
    max_proc = config["max_proc"] if config["max_proc"] < max_proc else max_proc
    max_trys = config["max_get_trys"]
    path_log = config["local"]["path_log"]
    path_result_raw = config["local"]["path_result_raw"]

    prep_dir([path_log, path_result_raw])
    path_log = path_log + "global.log"

    log(f"\nSTARTING ALL EXTRACTION PROCESSES", path_log, log_terminal=log_terminal)
 
    with multiprocessing.Pool(
        processes = (max_proc if len(setup) > max_proc else len(setup))
        ) as pool:
        with tqdm(total=len(setup), position=0, leave=True, disable=(not log_terminal)) as global_pbar:
            for _ in pool.imap_unordered(
                extract_json_from_url,
                [
                    (s, max_trys, path_log, path_result_raw, log_terminal)
                    for s in setup
                ],
                chunksize=1):
                global_pbar.update()

    log(f"FINISHED ALL EXTRACTION PROCESSES\n", path_log, log_terminal=log_terminal)


# single job - file compression (.parquet from .json file)
def compress_to_parquet(job, path_result_raw, path_result_compressed):
    pd.DataFrame.from_dict(
        read_raw_json(f'./{path_result_raw}{job["name"]}_{job["category"]}.json')
    ).to_parquet(
        f'{path_result_compressed}{job["name"]}_{job["category"]}.parquet'
    )


# interface - batch file compression
def compress(config, log_terminal=config["terminal_log"]):
    path_log = config["local"]["path_log"]
    path_result_raw = config["local"]["path_result_raw"]
    path_result_compressed = config["local"]["path_result_compressed"]

    prep_dir([path_log, path_result_compressed])
    path_log = path_log + "global.log"

    log(f"\nSTARTING COMPRESSION PROCESSES", path_log, log_terminal=log_terminal)
    for job in config["raw_source"]:
        prefix = f'JOB {job["name"]}, CATEGORY {job["category"]} -'
        try:
            compress_to_parquet(job, path_result_raw, path_result_compressed)
            log(f"{prefix} Finished file compression.", path_log, log_terminal=log_terminal)    
        except:
            log(f"{prefix} Error compressing file.", path_log, log_terminal=log_terminal)
    log(f"FINISHED COMPRESSION PROCESSES\n", path_log, log_terminal=log_terminal)



# interface - save to AWS S3clear
def send_batch_to_s3(config, batch_directory,  s3_directory_uri, log_terminal=config["terminal_log"]):

    path_log = config["local"]["path_log"] + "global.log"

    log(f"\nSTARTING AWS S3 UPLOAD PROCESSES FOR {batch_directory}", path_log, log_terminal=log_terminal)

    # matches files and jobs
    file_job_list = [
        (f, job)
        for job in config["raw_source"]
        for f in os.listdir(batch_directory)
        if f.split(".")[0] == f'{job["name"]}_{job["category"]}'
    ]

    # send the files individualy
    for (f, job) in file_job_list:
        prefix = f'JOB {job["name"]}, CATEGORY {job["category"]} -'
        try:
            send_to_s3(
                source_file_path=f'{batch_directory}{f}',
                directory_uri=s3_directory_uri,
                termianl_progress=log_terminal)
            log(f"{prefix} Finished sending to S3.", path_log, log_terminal=log_terminal)
        except:
            log(f"{prefix} Error sending to S3.", path_log, log_terminal=log_terminal)

    log(f"FINISHED UPLOAD PROCESSES FOR {batch_directory}\n", path_log, log_terminal=log_terminal)




# = = = = = EXECUTION = = = = =
    
# performing extraction
extract(config)

# compression
compress(config)

# raw data saving to AWS S3
if config["AWS"]["send_raw_to_s3"]: 
    send_batch_to_s3(
        config,
        config["local"]["path_result_raw"],
        config["AWS"]["destination_raw"],
        log_terminal=config["terminal_log"]
        )

# compressed data saving to AWS S3
if config["AWS"]["send_compressed_to_s3"]: 
    send_batch_to_s3(
        config,
        config["local"]["path_result_compressed"],
        config["AWS"]["destinaiton_compressed"],
        log_terminal=config["terminal_log"]
        )
