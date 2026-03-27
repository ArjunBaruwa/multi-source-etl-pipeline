"""
Multi-Source Data ETL Pipeline

Description:
This project demonstrates extracting data from multiple sources (CSV, JSON, XML),
transforming it, and loading it into a single CSV output file.

Author: Arjun Baruwal
"""

import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET
import glob

# -------------------------------
# Configuration
# -------------------------------
LOG_FILE = "log_file.txt"
TARGET_FILE = "transformed_data.csv"

# -------------------------------
# Logging Function
# -------------------------------
def log_progress(message):
    """Logs progress messages with timestamps."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, 'a') as f:
        f.write(f"{timestamp} - {message}\n")

# -------------------------------
# Extraction Functions
# -------------------------------
def extract_from_csv(file_to_read):
    """Extract data from CSV file."""
    return pd.read_csv(file_to_read)

def extract_from_json(file_to_read):
    """Extract data from JSON file (line-delimited)."""
    return pd.read_json(file_to_read, lines=True)

def extract_from_xml(file_to_read):
    """Extract data from XML file."""
    df = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_read)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        df = pd.concat([df, pd.DataFrame([{"name": name, "height": height, "weight": weight}])], ignore_index=True)
    return df

# -------------------------------
# Master Extract Function
# -------------------------------
def extract():
    """Extract data from all CSV, JSON, and XML files in folder."""
    extracted_data = pd.DataFrame(columns=['name', 'height', 'weight'])

    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, extract_from_xml(xmlfile)], ignore_index=True)

    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, extract_from_json(jsonfile)], ignore_index=True)

    for csvfile in glob.glob("*.csv"):
        if csvfile != TARGET_FILE:
            extracted_data = pd.concat([extracted_data, extract_from_csv(csvfile)], ignore_index=True)

    return extracted_data

# -------------------------------
# Transform Function
# -------------------------------
def transform(df):
    """Transform height from inches to meters, weight from pounds to kg."""
    df['height'] = round(df.height * 0.0254, 2)
    df['weight'] = round(df.weight * 0.45359237, 2)
    return df

# -------------------------------
# Load Function
# -------------------------------
def load_data(target_file, df):
    """Load transformed data into CSV file."""
    df.to_csv(target_file, index=False)

# -------------------------------
# Main ETL Pipeline
# -------------------------------
def main():
    log_progress("ETL Job Started")

    log_progress("Extract Phase Started")
    extracted_data = extract()
    log_progress("Extract Phase Ended")

    log_progress("Transform Phase Started")
    transformed_data = transform(extracted_data)
    print("\nTransformed Data:\n", transformed_data.head())
    log_progress("Transform Phase Ended")

    log_progress("Load Phase Started")
    load_data(TARGET_FILE, transformed_data)
    log_progress("Load Phase Ended")

    log_progress("ETL Job Completed Successfully")

# -------------------------------
# Entry Point
# -------------------------------
if __name__ == "__main__":
    main()