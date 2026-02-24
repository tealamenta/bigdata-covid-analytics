"""Setup configuration for bigdata-covid-analytics package."""

from setuptools import setup, find_packages

setup(
    name="bigdata-covid-analytics",
    version="1.0.0",
    author="Mohamed Horache",
    description="Big Data COVID-19 Analytics Pipeline",
    packages=find_packages(),
    python_requires=">=3.11",
    install_requires=[
        "pyspark>=3.5.3",
        "pandas>=2.2.0",
        "numpy>=1.26.0",
        "pyarrow>=14.0.1",
        "requests>=2.31.0",
        "elasticsearch>=8.11.0",
        "pyyaml>=6.0.1",
        "python-dotenv>=1.0.0",
        "pydantic>=2.5.0",
        "loguru>=0.7.2",
        "tqdm>=4.66.1",
        "click>=8.1.7",
    ],
    entry_points={
        'console_scripts': [
            'covid-ingest-france=src.ingestion.france_covid:main',
        ],
    },
)
