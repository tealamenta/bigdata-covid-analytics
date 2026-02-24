"""COVID-19 Data Indexing - Simple Version."""

import pandas as pd
from pathlib import Path
from elasticsearch import Elasticsearch


def main():
    print(" Starting Elasticsearch indexing...")
    
    es = Elasticsearch(["http://localhost:9200"])
    print(f" Connected to Elasticsearch")
    
    index_name = "covid-analytics"
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Deleted existing index")
    
    mapping = {
        "mappings": {
            "properties": {
                "date": {"type": "date", "format": "yyyy-MM-dd"},
                "country": {"type": "keyword"},
                "total_hospitalizations": {"type": "float"},
                "total_icu": {"type": "float"},
                "total_deaths": {"type": "float"},
                "total_recovered": {"type": "float"},
                "regions_count": {"type": "integer"},
            }
        }
    }
    es.indices.create(index=index_name, body=mapping)
    print(f" Created index: {index_name}")
    
    df = pd.read_csv("data/usage/covid_comparison/daily_comparison.csv")
    print(f" Read {len(df)} rows")
    
    success = 0
    for i, row in df.iterrows():
        try:
            if pd.isna(row["date"]):
                continue
            doc = {"date": row["date"], "country": row["country"]}
            for f in ["total_hospitalizations", "total_icu", "total_deaths", "total_recovered", "regions_count"]:
                if f in row and pd.notna(row[f]):
                    doc[f] = float(row[f])
            es.index(index=index_name, document=doc)
            success += 1
            if success % 200 == 0:
                print(f"   {success} indexed...")
        except Exception as e:
            if success < 3:
                print(f"Error: {e}")
    
    es.indices.refresh(index=index_name)
    count = es.count(index=index_name)["count"]
    print(f"\n Done! {count} documents indexed")
    print(f" Open Kibana, set time: 2020-01-01 to 2024-01-01")


if __name__ == "__main__":
    main()
