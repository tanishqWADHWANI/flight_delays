# Full ETL pipleline - Canada Flight Delays (2020-2025)

# importing the libraries 
import os
import pandas as pd
import numpy as np
import dask.dataframe as dd
import requests
from pathlib import Path
from google.cloud import bigquery
import time
import zipfile
from typing import List

# Extract: downloading data
def downloading_flight_data(start_year: int, end_year: int, output_dir: str | Path) -> List[Path]:
    """
    Download complete USDOT flight data without filtering
    """
    # Conifg
    DATA_DIR = Path(output_dir)
    DATA_DIR.mkdir(parents = True,exist_ok=True) # checks if the direcory already exists

    BASE_URL = "https://transtats.bts.gov/PREZIP/"

    current_file = 0
    total_files = (end_year - start_year + 1) *12 # for all years and months
    successful_downloads: List[Path] = []
    failed_downloads: List[str]= []

    print(f"Starting download from {start_year}-{end_year}")
    

    for year in range(start_year,end_year + 1):
        for month in range(1,13):
            current_file +=1
            filename = f"On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_{year}_{month}.zip"
            url = BASE_URL + filename
            file_path = DATA_DIR / filename

            

            # skip the file if already exists
            if file_path.exists():
                print(f"{current_file}/{total_files} {filename} Already exists.")
                successful_downloads.append(file_path)
                continue

            print(f"{current_file/total_files} Downloading {filename}....")

            try:
                # retrieves the data from the url using HTTP/FTP protocols
                response = requests.get(url,stream = True, timeout=300) 

                if response.status_code != 200:
                    print(f"File not available (Status): {response.status_code}")
                    failed_downloads.append(f"{year}-{month}") # added to the failed_downloads list above
                    continue

                # save file
                with open(file_path,'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192): # 8kb
                        if chunk: # ensures chunk is not empty
                            file.write(chunk)

                file_size = file_path.stat().st_size / (1024*1024) # size in MB

                print(f"Downloaded successfully: ({file_size:.1f}) MB")

                # for efficient server processing
                time.sleep(1)

            except Exception as e:
                print(f"Exception : {e}")
                failed_downloads.append(f"{filename}")
                # Clean up partial download
                if file_path.exists():
                    file_path.unlink()
                

    # formatting output (Print summary)
    print("\n"  + "="*60)
    print("DOWNLOAD SUMMARY")
    print("\n"  + "="*60)
    print(f"Total files attempted: {total_files}")
    print(f"Failed downloads: {len(failed_downloads)}")

    if failed_downloads:
        print(f"Failed months: {','.join(failed_downloads)}")

    return successful_downloads








            


if __name__ == "__main__":

    DATA_DIR = Path("data")
    downloading_flight_data(2020,2025,DATA_DIR)

