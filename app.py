from flask import Flask, jsonify
import pandas as pd
import os

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({
        "project": "COVID-19 Analytics - France vs Colombia",
        "author": "tealamenta",
        "endpoints": ["/stats", "/france", "/colombia"]
    })

@app.route('/stats')
def stats():
    return jsonify({
        "total_documents": 1270,
        "france": {
            "records": 1109,
            "percentage": "86.54%",
            "max_hospitalizations": 33466,
            "median_hospitalizations": 17531
        },
        "colombia": {
            "records": 171,
            "percentage": "13.46%",
            "median_deaths": 8
        },
        "period": "March 2020 - March 2023"
    })

@app.route('/france')
def france():
    return jsonify({
        "country": "France",
        "source": "data.gouv.fr",
        "records": 1109,
        "peak_hospitalizations": 33466,
        "peak_icu": 7019,
        "median_deaths": 89751
    })

@app.route('/colombia')
def colombia():
    return jsonify({
        "country": "Colombia",
        "source": "datos.gov.co",
        "records": 171,
        "median_deaths": 8,
        "note": "No hospitalization data available"
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    app.run(host='0.0.0.0', port=port)
