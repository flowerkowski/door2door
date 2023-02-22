# door2door

Architecture diagram

![image](https://user-images.githubusercontent.com/4832862/220592766-6257dc82-1556-46f7-b935-e21d07de6975.png)

## Databricks Notebook Documentation ##

### Overview ###

This Databricks notebook is designed to be run daily as part of a workflow, loading new files as specified by user-defined widgets. The notebook first calculates the previous day's date and retrieves user-defined widget values for the file location and type, as well as the process date (defaulting to June 1, 2019). It then loads the raw data into a DataFrame and saves it to a Delta Lake as the "bronze" table. The notebook then adds a new column to the DataFrame for date-only processing and flattens the nested JSON data. The flattened data is then filtered to only include records from the specified process date and saved as the "silver" table in Delta Lake. The notebook then demonstrates how to query and transform the "silver" table into separate "operating_period" and "vehicle" tables, merging new data for the current day with the previously stored Delta tables. Finally, the notebook includes an example of adding a window function to the "vehicle" table to calculate row numbers for each vehicle ID.

### Widgets ###

Three widgets are provided to allow users to specify parameters for the notebook:

*file_location*: A text widget specifying the location of the files to be loaded. Default value is s3://de-tech-assessment-2022/data.

*file_type*: A dropdown widget specifying the type of files to be loaded. Options are "csv", "parquet", and "json". Default value is "json".

*process_date*: A text widget specifying the date to be processed. Default value is "2019-06-01".

### Loading Raw Data ###

The notebook first calculates yesterday's date and retrieves the user-defined widget values for file location, file type, and process date. It then loads the raw data into a DataFrame named df_bronze, counts the number of records to be loaded, previews the data, and saves it to a Delta Lake as the "bronze" table.

### Preparing Data for Processing ###

The notebook adds a new column to the DataFrame called date_only to facilitate date-only processing. It then flattens the nested JSON data and selects only the records from the specified process date to create a new DataFrame named df_silver. The df_silver DataFrame is then saved to a Delta Lake as the "silver" table.

### Querying and Transforming Data ###

The notebook demonstrates how to query and transform the "silver" table to create separate "operating_period" and "vehicle" tables. It creates new DataFrames for the current day's data, adds temporary views for each, and merges the new data with the Delta tables using join conditions. Finally, the notebook adds a window function to the "vehicle" table to calculate row numbers for each vehicle ID.

### Conclusion ###
This Databricks notebook provides a template for loading, processing, and transforming daily data using user-defined widgets and Delta Lake tables. By following the notebook's structure and modifying the widgets and queries as needed, users can create customized daily workflows for their own data needs.
