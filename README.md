
# Music ETL Project

The Music ETL project is designed to create a comprehensive and automated data pipeline that integrates data from both the Spotify API and the LastFM API. The primary goal is to extract detailed information about artists, tracks, and albums, transform this data for analysis, and load it into a data mart for further insights. The project leverages Apache Airflow to schedule and automate these ETL processes, ensuring that the data is refreshed and up-to-date.


## Objectives

- Data Extraction: Retrieve detailed information on artists, tracks, and albums from the Spotify and LastFM APIs.
- Data Transformation: Clean and transform the extracted data to ensure consistency and usability.
- Data Loading: Load the transformed data into a data mart, stored in Parquet files, for efficient querying and analysis.
- Automation: Use Apache Airflow to automate the ETL process, ensuring that the data pipeline runs on a regular schedule.
## Data Sources

- Spotify API: Provides data about artists, tracks, and albums including popularity metrics, followers count, and preview URLs.
- LastFM API: Provides additional data about artists, tracks, and albums including listeners count, play count, genre tags, and release dates.
## ETL Workflow

#### Extract:

- Use the Spotify API to get information about artists, tracks, and albums.
- Use the LastFM API to get complementary information about the same entities.

#### Transform:

- Clean and normalize the data.
- Merge data from Spotify and LastFM to create a unified view.
- Extract relevant metrics and tags for analysis.

#### Load:

- Save the transformed data into Parquet files.
- Store the Parquet files in a data mart for efficient querying.

#### Automate:

- Use Apache Airflow to define and schedule the ETL tasks.
- Ensure the ETL pipeline runs daily and refreshes the data.
## Apache Airflow

Apache Airflow is used to automate and orchestrate the ETL workflow. The Airflow DAG (Directed Acyclic Graph) defines the sequence of tasks to be executed and their dependencies. The tasks include:

- Start: A dummy task to signify the start of the DAG.
- Run Notebooks: Execute Jupyter notebooks that perform the ETL operations.
- End: A dummy task to signify the end of the DAG.
## Project Structure

The project is organized into several directories:

- airflow/: Contains Airflow DAGs and configurations.
- data/: Contains input CSV files with artist and album names.
- notebooks/: Contains Jupyter notebooks for extracting, transforming, and loading data.
- scripts/: Contains Python scripts for data extraction, transformation, and loading.
- Mart/: Stores the output Parquet files with processed data.
- requirements.txt: Lists the Python dependencies required for the project.
- README.md: Provides an overview and setup instructions for the project.
## Prerequisites

- Python 3.6+
- Apache Airflow 2.0+
- Jupyter Notebook
- Spotify API Key
- LastFM API Key
## Installation and Setup

#### Clone the repository:

```bash
  git clone https://github.com/ronitguptaaa/MusicETL.git
cd MusicETL
```
#### Create and activate a virtual environment:

```bash
  python3 -m venv venv
source venv/bin/activate
```

#### Install the required packages:

```bash
  pip install -r requirements.txt
```

#### Install and initialize Apache Airflow:

```bash
  pip install apache-airflow
airflow db init
```

#### Set the Airflow home environment variable:

```bash
  export AIRFLOW_HOME=~/airflow
source ~/.zshrc  # or source ~/.bashrc
```

#### Create an Airflow user:

```bash
  airflow users create \
   --username admin \
   --firstname FIRST_NAME \
   --lastname LAST_NAME \
   --role Admin \
   --email admin@example.com
```

#### Copy the DAG file to the Airflow DAGs directory:

```bash
  cp airflow/dags/music_etl_dag.py $AIRFLOW_HOME/dags/
```

### Running Airflow

#### Start the Airflow web server:

```bash
airflow webserver --port 8080
```

#### Start the Airflow scheduler in another terminal:
```bash
airflow scheduler
```

Access the Airflow web UI at http://localhost:8080 to monitor and trigger DAGs.
## Usage/Examples

- Update data files (ArtistList.csv, TopGlobalArtists50.csv) in the data/ directory as needed.
- Modify notebooks in the notebooks/ directory for data processing.
- Run notebooks manually or schedule them using Airflow DAGs.
- Check processed data in the Mart/ directory.



## Conclusion

This project demonstrates an end-to-end ETL process for music data using APIs, Python, and Apache Airflow. By automating the ETL workflow, the project ensures that the data is continuously updated and ready for analysis, enabling insights into music trends and metrics.
## License

This project is licensed under the MIT License. See the LICENSE file for details.

