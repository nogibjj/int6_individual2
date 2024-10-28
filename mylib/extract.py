import os
import requests
import pandas as pd

def extract(
    url="https://raw.githubusercontent.com/fivethirtyeight/data/master/nutrition-studies/raw_anonymized_data.csv",
    file_path="data/Nutrition.csv",
    directory="data",
):
    """Extract a dataset from a URL and save it to a file path"""
    
    # Create directory if it doesn't exist
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Download the dataset from the URL
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Write the content to the specified file
        with open(file_path, "wb") as f:
            f.write(response.content)
    else:
        raise Exception(f"Failed to download data. Status code: {response.status_code}")

    # Load the CSV file into a DataFrame
    df = pd.read_csv(file_path)
    cols = [
        'ID', 'cancer', 'diabetes', 'heart_disease',
        'EGGSFREQ', 'GREENSALADFREQ', 'FRIESFREQ', 
        'MILKFREQ', 'SODAFREQ', 'COFFEEFREQ', 'CAKESFREQ'
    ]
    df_subset = df.head(100)[cols]
    df_subset.to_csv(file_path, index=False)
    
    return file_path
