# Data Ingestion for all three datasets in Commands 

# create folder where you will ingest datasets into 
hdfs dfs -mkdir originalDataSets
# verify that you created your folder 
hdfs dfs -ls 
# create folder lead_level_test 
hdfs dfs -mkdir originalDataSets/lead_level_test
# put the dataset about Childhood Blood Lead Testing and Elevated Incidence by Zip Code: Beginning 2000 in this folder
hdfs dfs -put children_blood_lead_level.csv originalDataSets/lead_level_test
# verify dataset is in folder 
hdfs dfs -ls originalDataSets/lead_level_test
# create folder housing_conditions
hdfs dfs -mkdir originalDataSets/housing_conditions
# put dataset about Housing Maintenance Code Violations in this folder
hdfs dfs -put housing_con.csv originalDataSets/housing_conditions
# verify dataset is in folder 
hdfs dfs -ls originalDataSets/housing_conditions
# create folder economics_data
hdfs dfs -mkdir originalDataSets/economics_data
# put dataset about Quarterly Census of Employment and Wages Annual Data: Beginning 2000 in this folder
hdfs dfs -put econ_data.csv originalDataSets/economics_data
# verify dataset is in the folder 
hdfs dfs -ls originalDataSets/economics_data