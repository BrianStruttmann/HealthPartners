import requests
import re
import csv
import io
import pandas as pd
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


METASTORE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
METADATA_PATH = Path.cwd() / "run_list.csv"


def get_last_run_date():
    """
    Find the most recent run date read from the metadata file. If the file doesn't exist, create it.  If
    there are no records in the file, start processing from the beginning (in this case 1/1/1900)
    """
    if not METADATA_PATH.exists():
        header = ["run_date", "processed_count"]
        with METADATA_PATH.open(mode="w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)

    df = pd.read_csv(METADATA_PATH, parse_dates=["run_date"])

    max_date = df["run_date"].max()

    # If didn't find a date or this is the first time the script is run, use a historical date
    if pd.isnull(max_date):
        max_date = datetime(1900, 1, 1)
    #
    #
    #  Override for testing, this forces some data to be returned from the metadata store no matter today's date.  
    #  Remove the below line when testing is complete
    #max_date = datetime(2025, 4, 20)
    #
    #
    #
    return max_date


def get_file_data(url):
    """
    Read the data from the metastore path hardcoded as METASTORE_URL above
    """
    response = requests.get(url)
    return response.json()


def filter_hospital_entries(dataset_list):
    """
    Given the list of datasets, return only those where the "theme" is "Hospitals"
    """
    hospital_datasets = []
    for d in dataset_list:
        try:
            if "Hospitals" in d["theme"]:
                hospital_datasets.append(d)
        except (KeyError, TypeError):
            continue 
    return hospital_datasets


def filter_modified_entries(dataset_list, last_run_date):
    """
    Given the list of datasets, return only those where the "modified" value is greater than the value
    saved from the last time this script ran
    """
    modified_datasets = []
    for d in dataset_list:
        try:
            mod = datetime.fromisoformat(d["modified"])
        except (KeyError, TypeError, ValueError):
            continue  # skip entries without valid "modified"
        if mod > last_run_date:
            modified_datasets.append(d)

    return modified_datasets


def to_snake_case(name):
    """
    This function is run against a single column from an input file. We remove any special
    characters and convert spaces to underscores.
    """
    name = name.strip()
    name = re.sub(r"[’'\",()\[\]{}]", "", name)
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name)
    return name.lower().strip("_")


def process_file(url):
    """
    Given a file's URL, read the file, standardize its header, and write the resulting file
    to our local "output" directory
    """

    response = requests.get(url)
    response.raise_for_status()

    # Read entire CSV
    csv_text = response.content.decode("utf-8")
    reader = csv.reader(io.StringIO(csv_text))
    rows = list(reader)

    # Split header and data
    header, data_rows = rows[0], rows[1:]

    # Process the header
    processed_header = [to_snake_case(col) for col in header]

    # Write new CSV
    output_path = Path.cwd() / "output" / url.rsplit("/", 1)[-1]
    
    with output_path.open(mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(processed_header)
        writer.writerows(data_rows)

    return f"File complete: {output_path}"


def update_run_metadata(run_date, processed_count):
    """
    This function runs after successfully processing all of the files.  It appends today's date
    and the number of files processed to the Metadata file.
    """
    
    new_row = {
        "run_date": run_date.strftime("%Y-%m-%d"),
        "processed_count": processed_count
    }
    with open(METADATA_PATH, mode="a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["run_date", "processed_count"])
        writer.writerow(new_row)


def main():
    """
    This is the main processing function for the script.  This script reads the CMS provider data metastore, 
    finds any entries with the "Hospitals" theme, and processes any files modified since the last time this script ran.
    """

    last_run_date = get_last_run_date()
    print(f"Processing datasets modified after {last_run_date.strftime('%Y-%m-%d')}...")

    # Read the data from the metastore
    all_datasets = get_file_data(METASTORE_URL)

    # Filter on the themes from the returned dataset, returning only the themes with "Hospitals"
    hospital_datasets = filter_hospital_entries(all_datasets)
    
    # Now filter to include only items that were modified after this script last ran
    modified_datasets = filter_modified_entries(hospital_datasets, last_run_date)
    
    url_list = []
    processed_file_count = 0

    # Build the list of URLs that we will process now that the filters have been applied
    for dataset in modified_datasets:
        url = dataset["distribution"][0]["downloadURL"].strip()
        url_list.append(url)

    if len(url_list) > 0:
        # Ensure the output folder is available
        output_folder = Path.cwd() / "output"
        output_folder.mkdir(exist_ok=True)

        # Use ThreadPoolExecutor to run up to 50 files in parallel
        with ThreadPoolExecutor(max_workers=50) as executor:
            # Submit all tasks to the pool
            future_to_file = {executor.submit(process_file, f): f for f in url_list}

            # As each one finishes, handle the result
            for future in as_completed(future_to_file):
                file = future_to_file[future]
                try:
                    processed_file_count += 1
                    result = future.result()
                    print(result)
                except Exception as e:
                    print(f"Error processing {file}: {e}")        

    print(f"Number of All Datasets      = {len(all_datasets)}")
    print(f"Number of Hospital Datasets = {len(hospital_datasets)}")
    print(f"Number of Modified Datasets = {len(modified_datasets)}")

    update_run_metadata(datetime.today(), processed_file_count)

    print(f"DONE! Successfully processed {processed_file_count} files today.")



if __name__ == "__main__":
    main()
