# Project - YOUTUBE DATA HARVESTING AND WAREHOUSING

YouTube Data Harvesting and Warehousing is a project aimed at developing a user-friendly Streamlit application that leverages the power of the Google API to extract valuable information from YouTube channels. The extracted data is then stored in a MongoDB database, subsequently migrated to a SQL data warehouse, and made accessible for analysis and exploration within the Streamlit app.

**Key Technologies and Skills**

PYTHON:
Python is a powerful programming language renowned for being easy to learn and understand. Python is the primary language employed in this project for the development of the complete application, including data retrieval, processing, analysis, and visualisation.

GOOGLE API CLIENT:
The googleapiclient library in Python facilitates the communication with different Google APIs. Its primary purpose in this project is to interact with YouTube's Data API v3, allowing the retrieval of essential information like channel details, video specifics, and comments. By utilizing googleapiclient, developers can easily access and manipulate YouTube's extensive data resources through code.

MONGODB:
MongoDB is built on a scale-out architecture that has become popular with developers of all kinds for developing scalable applications with evolving data schemas. As a document database, MongoDB makes it easy for developers to store structured or unstructured data. It uses a JSON-like format to store documents.

POSTGRESQL:
PostgreSQL is an open-source, advanced, and highly scalable database management system (DBMS) known for its reliability and extensive features. It provides a platform for storing and managing structured data, offering support for various data types and advanced SQL capabilities.

STREAMLIT:
Streamlit library was used to create a user-friendly UI that enables users to interact with the programme and carry out data retrieval and analysis operations.

**REQUIRED LIBRARIES:**

- googleapiclient.discovery

- streamlit

- psycopg2

- pymongo

- pandas

**FEATURES:**

- Retrieve data from the YouTube API, including channel information, playlists, videos, and comments.
- Store the retrieved data in a MongoDB database.
- Migrate the data to a SQL data warehouse.
- Analyze and visualize data using Streamlit and Plotly.
- Perform queries on the SQL data warehouse.
- Gain insights into channel performance, video metrics, and more.
