# door2door

Architecture diagram

![image](https://user-images.githubusercontent.com/4832862/220592766-6257dc82-1556-46f7-b935-e21d07de6975.png)

## door2door Databricks Notebook Documentation ##

This notebook contains a workflow to load data from an S3 bucket, process it, and save the processed data in Delta Lake tables. It includes widgets to specify the process date, the S3 bucket location, and the type of files to be loaded. The notebook processes JSON files by default, but the file type can be changed using the dropdown widget.

###Widgets###
file_location: A text widget used to specify the location of the S3 bucket where the data files are stored. The default value is s3://de-tech-assessment-2022/data.
file_type: A dropdown widget used to specify the type of files to be loaded. The options are CSV, Parquet, and JSON, with JSON being the default value.
process_date: A text widget used to specify the date of the data to be processed. The default value is 2019-06-01.
Workflow
Calculate yesterday: Calculates the day before the current date, which will be used in subsequent steps to load the data for the previous day. The calculated date is printed to the console for verification.
Load the RAW data: Loads the raw data from the S3 bucket into a Spark DataFrame using the specified file type and location.
Exploratory Data Analysis: Counts the number of records in the raw DataFrame.
Preview the RAW data: Shows a preview of the raw DataFrame.
Save the RAW data to a Delta Lake: Saves the raw DataFrame as a Delta Lake table named door2door_table_bronze.
Add Date only column: Adds a new column to the raw DataFrame containing only the date, which will be used to find new records to be processed.
Create the Silver dataset: Selects the relevant columns from the raw DataFrame, flattens the nested JSON, and filters for records with the specified process date. The resulting DataFrame is saved as a Delta Lake table named door2door_table_silver.
Preview the Silver Data: Shows a preview of the silver DataFrame using Spark SQL.
Check how we can de-normalize the one big table into a more logical data model: Performs a query to show how the silver DataFrame can be denormalized into separate tables for operating period and vehicle data.
Create separate table for Operating period data: Creates a Delta Lake table named operating_period containing the operating period data.
Create separate Delta table for vehicle data: Creates a Delta Lake table named vehicle containing the vehicle data.
Create Dataframes for the new day data: Creates new DataFrames containing the operating period and vehicle data for the current day.
Merge new data into Delta table for vehicle data: Uses a temporary view and a join condition to merge the new vehicle data with the existing Delta table for vehicle data. The merged data is then appended to the Delta table.
Merge new data into Delta table for operating period data: Uses a temporary view and a join condition to merge the new operating period data with the existing Delta table for operating period data. The merged data is then appended to the Delta table.
Experimenting with adding window function to calculate the row number for each vehicle id: Adds a row number to each row in the vehicle DataFrame using a window function. The resulting DataFrame is printed to the console for verification.
