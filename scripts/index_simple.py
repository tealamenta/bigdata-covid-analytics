"""Simple Elasticsearch indexing script."""

import pandas as pd
from elasticsearch import Elasticsearch
import os
import math

def main():
    print("Starting Elasticsearch indexing...")
    
    # Connect to Elasticsearch
    es = Elasticsearch(
        ["http://localhost:9200"],
        basic_auth=("elastic", "changeme")
    )
    
    if not es.ping():
        print("Cannot connect to Elasticsearch")
        return
    
    print("Connected to Elasticsearch")
    
    # Delete existing index
    if es.indices.exists(index="covid-analytics"):
        es.indices.delete(index="covid-analytics")
        print("Deleted existing index")
    
    # Create index
    es.indices.create(index="covid-analytics")
    print("Created index: covid-analytics")
    
    # Read CSV from Spark output (it's a folder)
    csv_path = "data/usage/covid_comparison/daily_comparison.csv"
    
    # Find the actual CSV file inside the folder
    if os.path.isdir(csv_path):
        for f in os.listdir(csv_path):
            if f.endswith(".csv") and not f.startswith("."):
                csv_file = os.path.join(csv_path, f)
                break
    else:
        csv_file = csv_path
    
    print(f"Reading: {csv_file}")
    df = pd.read_csv(csv_file)
    
    # Replace NaN with None
    df = df.fillna(0)
    
    print(f"Loaded {len(df)} records")
    
    # Index documents
    indexed = 0
    for _, row in df.iterrows():
        doc = row.to_dict()
        # Clean NaN values
        clean_doc = {}
        for k, v in doc.items():
            if pd.isna(v) or (isinstance(v, float) and math.isnan(v)):
                clean_doc[k] = 0
            else:
                clean_doc[k] = v
        es.index(index="covid-analytics", document=clean_doc)
        indexed += 1
    
    print(f"Indexed {indexed} documents")
    print("SUCCESS - Indexing completed!")

if __name__ == "__main__":
    main()
