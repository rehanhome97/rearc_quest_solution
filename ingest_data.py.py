# Databricks notebook source
# DBTITLE 1,Setup Unity Catalog Volumes
# MAGIC %sql
# MAGIC -- Create catalog and schema if they don't exist
# MAGIC CREATE CATALOG IF NOT EXISTS rearc_quest;
# MAGIC CREATE SCHEMA IF NOT EXISTS rearc_quest.quest_1;
# MAGIC
# MAGIC -- Create volumes for raw data
# MAGIC CREATE VOLUME IF NOT EXISTS rearc_quest.quest_1.raw_data;
# MAGIC
# MAGIC -- Verify volumes were created
# MAGIC SHOW VOLUMES IN rearc_quest.quest_1;

# COMMAND ----------

# DBTITLE 1,Configuration and Imports
# Imports
import requests
from bs4 import BeautifulSoup
import os
import json

# Configuration
headers = {"User-Agent": "DataEngineer (rehanhome97@gmail.com)"}

# Unified volume path for all raw data
RAW_DATA_PATH = "/Volumes/rearc_quest/quest_1/raw_data/"

# Data source URLs
BLS_BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
POPULATION_API_URL = "https://api.worldbank.org/v2/country/USA/indicator/SP.POP.TOTL?format=json&per_page=100"

# Helper functions
def write_to_volume(content, volume_path, filename):
    """Write content to volume via temporary workspace location."""
    workspace_temp = f"/Workspace/Users/rehanhome97@gmail.com/.temp/{filename}"
    dbutils.fs.mkdirs("/Workspace/Users/rehanhome97@gmail.com/.temp/")
    dbutils.fs.put(workspace_temp, content, overwrite=True)
    dbutils.fs.cp(workspace_temp, os.path.join(volume_path, filename), recurse=False)
    dbutils.fs.rm(workspace_temp)

def get_remote_file_size(url, headers):
    """Get remote file size without downloading."""
    try:
        response = requests.head(url, headers=headers, allow_redirects=True)
        return int(response.headers.get('Content-Length', 0))
    except:
        return None

def get_local_file_size(file_path):
    """Get local file size from volume."""
    try:
        file_info = dbutils.fs.ls(file_path)
        return file_info[0].size if file_info else None
    except:
        return None

def should_download(remote_url, local_path, headers):
    """Check if file needs downloading (new or changed)."""
    remote_size = get_remote_file_size(remote_url, headers)
    local_size = get_local_file_size(local_path)
    
    # Download if: file doesn't exist locally OR sizes differ
    return local_size is None or remote_size != local_size

# COMMAND ----------

# DBTITLE 1,BLS Data Ingestion
def scrape_bls_directory(base_url, headers):
    """Scrape BLS HTML directory listing to get file links."""
    response = requests.get(base_url, headers=headers)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'html.parser')
    file_links = []
    
    # Parse links - extract filenames that start with 'pr.'
    for link in soup.find_all('a'):
        href = link.get('href', '')
        filename = href.split('/')[-1]
        if filename.startswith('pr.'):
            file_links.append(filename)
    
    return file_links

def download_bls_data(base_url, output_dir, headers):
    """Download BLS productivity data to unified volume (skip unchanged files)."""
    dbutils.fs.mkdirs(output_dir)
    file_links = scrape_bls_directory(base_url, headers)
    
    downloaded_count = 0
    skipped_count = 0
    
    for filename in file_links:
        remote_url = base_url + filename
        local_path = os.path.join(output_dir, filename)
        
        # Check if file needs downloading
        if not should_download(remote_url, local_path, headers):
            print(f"  ⊙ {filename} (unchanged, skipped)")
            skipped_count += 1
            continue
        
        try:
            response = requests.get(remote_url, headers=headers)
            response.raise_for_status()
            write_to_volume(response.content.decode('latin-1'), output_dir, filename)
            print(f"  ✓ {filename} ({len(response.content)} bytes)")
            downloaded_count += 1
        except Exception as e:
            print(f"  ✗ {filename}: {e}")
    
    print(f"\n{downloaded_count} downloaded, {skipped_count} skipped (unchanged)")
    return downloaded_count

# Execute BLS data ingestion
print("=" * 60)
print("BLS PRODUCTIVITY DATA INGESTION")
print("=" * 60)
download_bls_data(BLS_BASE_URL, RAW_DATA_PATH, headers)

# COMMAND ----------

# DBTITLE 1,Population API Data Ingestion
def download_population_data(api_url, output_dir, headers):
    """Fetch population data from API and save as JSON to unified volume (skip if unchanged)."""
    filename = "population.json"
    local_path = os.path.join(output_dir, filename)
    
    try:
        # Fetch new data
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        data = response.json()
        new_content = json.dumps(data, indent=2)
        
        # Check if local file exists and compare content
        try:
            existing_file = dbutils.fs.head(local_path, 10 * 1024 * 1024)  # Read up to 10MB
            if existing_file == new_content:
                record_count = len(data[1]) if isinstance(data, list) and len(data) > 1 else 0
                print(f"  ⊙ {filename} (unchanged, skipped - {record_count} records)")
                return True
        except:
            pass  # File doesn't exist or can't read, proceed with download
        
        # Write new/changed data
        write_to_volume(new_content, output_dir, filename)
        record_count = len(data[1]) if isinstance(data, list) and len(data) > 1 else 0
        print(f"  ✓ {filename} ({len(new_content)} bytes, {record_count} records)")
        return True
    except Exception as e:
        print(f"  ✗ {filename}: {e}")
        return False

# Execute Population API ingestion
print("\n" + "=" * 60)
print("POPULATION DATA INGESTION")
print("=" * 60)
download_population_data(POPULATION_API_URL, RAW_DATA_PATH, headers)

# COMMAND ----------

# DBTITLE 1,Verify Ingested Data
print("\n" + "=" * 60)
print("VERIFICATION - Ingested Files")
print("=" * 60)

# List all files in unified raw_data volume
print("\nAll files in unified raw_data volume:")
print("-" * 60)
all_files = dbutils.fs.ls(RAW_DATA_PATH)

for file_info in sorted(all_files, key=lambda x: x.name):
    size_kb = file_info.size / 1024
    file_type = "Population" if file_info.name == "population.json" else "BLS"
    print(f"  {file_info.name:<30} {size_kb:>8.2f} KB  [{file_type}]")

print(f"\n  Total files: {len(all_files)}")
print(f"  Total size: {sum(f.size for f in all_files) / (1024*1024):.2f} MB")

print("\n" + "=" * 60)
print("✓ Data ingestion complete and verified!")
print("=" * 60)