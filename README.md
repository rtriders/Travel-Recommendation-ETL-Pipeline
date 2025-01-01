# Travel Recommendation ETL Pipeline

### Overview

The Travel-Recommendation-ETL-Pipeline is a data processing workflow designed to analyze weather conditions from various global cities and generate insights for travel recommendations. The pipeline evaluates cities based on a Comfort Index, ranking them to help users identify optimal travel destinations.
 
 ### Architecture
 ![Architecture Diagram](https://github.com/rtriders/Travel-Recommendation-ETL-Pipeline/blob/main/Travel_Recomenndation_Pipeline_Architecture.jpeg)
 
 ### About OpenWeather API
 The [OpenWeather API](https://openweathermap.org/guide) is a comprehensive weather data service that provides real-time, historical, and forecast weather information for locations worldwide. It is widely used by developers, businesses, and researchers to integrate weather-related functionalities into their applications or systems.
 
 In this project we have used the [**Major 50 - Cities**] as the data for our piepline.
 
 ### Services Used
 
1. [**Amazon S3**](https://docs.aws.amazon.com/s3/index.html): Amazon Simple Storage Service (S3) is a cloud-based object storage service provided by Amazon Web Services (AWS). It is designed to store and retrieve large amounts of data, such as photos, videos, and other types of files, from anywhere on the web
2. [**CloudWatch**](https://docs.aws.amazon.com/cloudwatch/): Amazon CloudWatch is a monitoring and observability service provided by Amazon Web Services (AWS). It is designed to collect and track metrics, collect and monitor log files, and set alarms
3. [**AWS Glue Crawler**](https://docs.aws.amazon.com/glue/latest/webapi/API_Crawler.html): AWS Glue Crawler is a service that automatically crawls data stores, such as Amazon S3, RDS, and Redshift, to infer schemas and partition structures. It can also identify changes in schemas and update the Data Catalog accordingly.
4. [**AWS Lambda**](https://docs.aws.amazon.com/lambda/index.html): AWS Lambda is a serverless computing service provided by Amazon Web Services (AWS). It enables developers to run code without provisioning or managing servers. Lambda runs code in response to events and automatically scales up or down to match the incoming request traffic.
5. [**AWS Athena**](https://docs.aws.amazon.com/athena/): Amazon Athena is an interactive query service provided by Amazon Web Services (AWS) that makes it easy to analyze data stored in Amazon S3 using standard SQL. It allows users to quickly and easily query data without having to set up and manage complex infrastructure or data warehousing tools.
6. [**Data Catalog**](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html): The Data Catalog is a central repository that stores metadata about data sources, tables, and transforms, which can be queried and managed by other AWS services, such as Athena and EMR.

### Install Packages
```
pip install pandas
pip install boto3
pip install numpy
import requests

```

### Execution Flow

1. Data Extraction: The pipeline extracts weather data through the OpenWeather API.
   
3. Trigger Mechanism: AWS CloudWatch Event Rule triggers a Lambda function daily to start the pipeline.
4. Raw Data Storage: The Lambda function extracts weather data in JSON format and stores it in the raw folder in an S3 bucket.
5. Data Transformation: A second Lambda function processes the raw data by cleansing, normalizing, and calculating metrics (e.g., Comfort Index).The transformed data is stored in the transformed folder in S3 in CSV format.
6. Data Cataloging: AWS Glue Crawler scans the transformed data in S3, updates the schema, and stores metadata in the AWS Glue Data Catalog.
7. Data Querying and Analysis:Amazon Athena enables SQL queries on the transformed data stored in the Glue Data Catalog.Queries provide insights such as travel recommendations, city comfort rankings, and weather patterns.


### Sample Output

Sample Output:
City	Comfort Index	Rank
Sydney	0.87	1
Cape Town	0.85	2
Tokyo	0.82	3
