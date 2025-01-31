## **ETL Pipeline**

### Project Overview
This project extracts trending video data from the YouTube API of various regions, processes it to identify top tags linked to video categories of trending videos, and then loads the data to an Azure Data Lake Storage (ADLS) Gen 2 container. The pipeline is built using Python, Apache Airflow, and PySpark, and is designed to be scalable and efficient daily batch runs.

### Features
- Data Extraction: Fetches trending video data of various regions using the YouTube data API v3.
- Data Transformation: Processes and transforms the raw video data to extract and analyze top tags associated with video categories.
- Data Loading: Loads the transformed data into an ADLS Gen 2 container for further analysis or reporting.

### Tools and Technologies
- Python: Programming language used for the core logic of the ETL pipeline.
- Apache Airflow: Used for orchestrating and scheduling the ETL workflow.
- PySpark: Used for large-scale data processing and transformation.
- YouTube Data API: Provides access to trending video data.
- Azure Data Lake Storage Gen 2 (ADLS Gen 2): Cloud storage used for storing the processed data.
