# Data Engineering Zoomcamp Homeworks

- [DataTalks Club](https://datatalks.club/) driven course on Data Engineering
- Further info in [DataTalks.Club's Slack](https://datatalks.club/slack.html)
- The videos instructions and office hours are published to [DataTalks.Club's YouTube channel](https://www.youtube.com/c/DataTalksClub) in [the course playlist](https://www.youtube.com/playlist?list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb) 


### [Week 1: Introduction & Basics](week_01)

- Set up and practice of Docker, Terraform; Google Cloud Platform basics
- [Homework](week_01/docker_sql/): Docker for containerized PostgresQL DB, Python scripts to ingest data, SQL sripts for basic analysis of NY Taxi data (https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)


### [Week 2: Data ingestion](week_02)

- Set up cloud infrastructure (Google Cloud Storage and Big Query)
-  [Homework](week_02/airflow/dags_new/): Create Airflow DAGs to load NY Yellow Taxi Trip Records and For-Hire Vehicle Trip Records for 2019 and 2020


### [Week_3: Data Warehous on Google Big Query](week_03)

- Updated Airflow DAGs to ingest NY For-Hire Vehicle data and force schema for yellow taxi data (week_03/airflow)
- [Homework](week_03/dwh): SQL queries to create clustered and partitioned table and compare data volume processed querying it and the raw table
