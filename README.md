## Summary

The analytics team need to work with data about users and song of Sparkify, a music streaming app. 
All data is in .json format and storaged in S3 and the Data Engineer was asked to work with them to build a ETL pipeline that extracts the data from S3, stages them in Redshift and transforms data into a set of dimensional tables.

## About Dataset

How said above, all data is in .json format storage in S3. The links are:

* Song data: s3://udacity-dend/song_data
* Log data: s3://udacity-dend/log_data
* Log data json path: s3://udacity-dend/log_json_path.json

## About Tables

All the data was managed to be in a star schema, an easy way to execute query and extract results.

#### Fact Table

songplays - records in event data associated with song plays

* columns - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
     
#### Dimension Tables

users - users in the app
 * columns - user_id, first_name, last_name, gender, level
    
songs - songs in music database
 * columns - song_id, title, artist_id, year, duration
    
artists - artists in music database
 * columns - artist_id, name, location, lattitude, longitude
    
time - timestamps of records in songplays broken down into specific units
 * columns - start_time, hour, day, week, month, year, weekday
    
 ## About Files
 
 * create_table.py - all the dimensions and fact tables are created
 * etl.py - the data is loaded from s3 to a staging tables and inserted in dimensions and fact tables
 * sql_queries - all SQL commands used in etl.py and create_table.py
 * dwh.cfg - parameters to create the cluster
 * aws_configuration.ipynb - cluster, s3, IAM configurations
 
  The file create_table.py above need to be run first to drop all exiting tables and create another new ones. Done this, the etl.py can be executed.