# Shape's Data Engineering Interview Tech Case
This technical case for the data engineering interview involves an **FPSO** vessel with various pieces of equipment, each equipped with multiple sensors. When a failure occurs, sensor data from the malfunctioning equipment is stored in a log file (note: all times are in GMT).

You are given 3 files: a log file named “**equipment_failure_sensors.txt**”; a file named “**equipment_sensors.csv**” which maps sensors to equipment; and a file named “**equipment.json**” which contains data about the equipment.

### Note: You can complete this test either *BEFORE* or *DURING* the tech interview.

> ## Steps

> 1. **Fork** or **Clone** this repository before starting the tech test.

> 2. *Unzip **equipment_failure_sensors.rar** to a path/folder where you can access the .txt data.*

> 3. You **must** structure your data in a way that optimizes queries, focusing on efficient data retrieval involving equipment, sensors, and dates. Data manipulation in **ACID** transactions is also expected.

> 4. **Python, SQL, and/or PySpark should be your primary languages/frameworks - you can use other tools if necessary for recommended and/or additional actions.**

> 5. **You're free to modify the files into any extension, provided the core data remains intact. If you need to modify the source data, ensure you use a copy method to keep the new data decoupled from the original data.**

Your solutions should answer the following:

1. What's the total number of equipment failures that occurred?
2. Which piece of equipment experienced the most failures?
3. What's the average number of failures across the equipment group, ordered by the number of failures in ascending order?
4. Rank the sensors by the number of errors they reported, grouped by equipment name in an equipment group.

### Recommendations: 

>- Structure your pipeline using OOP principles.
>- Follow PEP or Black coding style.
>- Use a GIT provider for code versioning.
>- Provide a diagram of your solutions.

### Bonus:

>- Write tests.
>- Simulate a streaming pipeline.
>- Dockerize your application.
>- Design an orchestration architecture.

## Solution

The following architecture was implemented to solve this challenge:

![Architecture](img/architecture.png)

# Getting Started

Follow the steps below to set up and run this project on your local environment.

## Prerequisites

- Python 3.8+
- Docker and Docker Compose (For running Airflow)
- Git
- Azure Storage Account
- Databricks

## Configuration Steps

1. **Clone the Repository**: First, clone this repository into your local environment using the following Git command:

```
git clone git@github.com:cadu1996/challenge-de-interview.git
```

## Using Docker and Docker Compose to Run Airflow

If you want to run the application using Docker and Docker Compose for Airflow, follow the steps below:

1. **Build and Start Docker Compose Services**: In the root folder of the project, build and start the Docker Compose services using the following command:

```
docker-compose up -d
```

This command will download the necessary Docker images, build the containers, and start the Airflow services.

2. **Access Airflow**: After the services have started, you can access the Airflow interface by opening your browser and going to `localhost:8080`.

The Airflow interface requires you to log in. Use `airflow` for both the username and password.

3. **Set Up Airflow Connection**: Once you're logged in, you need to create a connection within Airflow for your data sources. Follow the instructions in the Airflow documentation to set up the necessary connections.

4. **Create Databricks Tasks**: You need to set up tasks in Databricks for the following files:


- `data_analytics/failure_log_analysis.py`: This script contains the logic for analyzing the equipment failure log data. It includes the code for generating the answers to the challenge questions.

- `data_engineering/raw_to_prepared/failure_log_table.py`: This script is responsible for moving and transforming data from the "raw" layer to the "prepared" layer of the data lake. It applies specific transformations designed to structure the data in a way that facilitates answering the challenge questions.

- `data_engineering/staging_to_raw/equipment_failure_sensors.py`: This script moves data from the "staging" layer to the "raw" layer of the data lake. It applies basic transformations to the sensor data related to equipment failures, preparing it for further processing.

- `data_engineering/staging_to_raw/equipment.py`: This script transfers data about the equipment from the "staging" layer to the "raw" layer of the data lake. It also applies basic transformations to the data, preparing it for the next stage of processing.

- `data_engineering/staging_to_raw/equipment_sensors.py`: This script moves the data regarding the relationship between equipment and sensors from the "staging" layer to the "raw" layer in the data lake. It applies basic transformations to prepare the data for subsequent analysis stages.

These scripts form the basis of the data pipeline in this project, facilitating the movement and transformation of data from one layer of the data lake to another, and finally enabling the analysis of the data to answer the challenge questions.

Now you should be able to see the application running and Airflow operational.



### Data Lake Structure

The data lake was created using Storage Account, Databricks, and Delta Lake. It's structured into different layers, each with a specific purpose:

- **Staging**: This is where data is initially stored without any processing or transformations. It's organized by ingestion date (year, month, and day).

- **Raw**: At this layer, data undergoes preliminary transformations such as data type definition, conversion to parquet format, and other necessary transformations to make the data usable.

- **Prepared**: In this final layer, the data is stored in Delta Lake format and organized to answer specific questions and facilitate faster queries.

