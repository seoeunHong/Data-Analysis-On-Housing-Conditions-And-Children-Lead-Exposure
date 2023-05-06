-- Creates an external table called 'children_lead_levels' with four columns: 'county', 'year', 'less_than_5', and '5_to_10'  based on the Spark output written to the HDFS directory at this location
-- hdfs://nyu-dataproc-m/user/nls406_nyu_edu/finalCode/lead/. 

use nls406_nyu_edu;

CREATE EXTERNAL TABLE 
children_lead_levels(county STRING, year STRING, less_than_5 INT, 5_to_10 INT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE location  'hdfs://nyu-dataproc-m/user/nls406_nyu_edu/finalCode/lead/';

-- Obtain the metadata of the children_lead_levels table. 

DESCRIBE children_lead_levels;

-- Find all the distinct values of the column “county” in the children_lead_levels table

SELECT DISTINCT county FROM children_lead_levels;

-- Find all the distinct values of the column “year” in the children_lead_levels table

SELECT DISTINCT year FROM children_lead_levels;

-- Find all the distinct values of the column “less_than_5” in the children_lead_levels table

SELECT DISTINCT less_than_5 FROM children_lead_levels;

-- Find all the distinct values of the column “5_to_10” in the children_lead_levels table

SELECT DISTINCT 5_to_10 FROM children_lead_levels;

-- Counts the number of counties in the 'children_lead_levels' table

SELECT COUNT(county) FROM children_lead_levels;

-- Counts the number of years in the ‘children_lead_levels’ table

SELECT COUNT(year) FROM children_lead_levels;

-- Find the mean number of children with lead exposure less than less than 5 mcg/dL

SELECT AVG(less_than_5) FROM children_lead_levels;

-- Find the mode number of children with lead exposure less than less than 5 mcg/dL

SELECT less_than_5, COUNT(*) as mode FROM children_lead_levels GROUP BY less_than_5 ORDER BY mode DESC LIMIT 1;

-- Find the median number of children with lead exposure less than less than 5 mcg/dL

SELECT percentile(less_than_5, 0.5) as median FROM children_lead_levels;

-- Find the mean number of children with lead exposure within 5-10 mcg/dL 

SELECT AVG(5_to_10) FROM children_lead_levels;

-- Find the mode number of children with lead exposure within 5-10 mcg/dL 

SELECT 5_to_10, COUNT(*) as mode FROM children_lead_levels GROUP BY 5_to_10 ORDER BY mode DESC LIMIT 1;

-- Find the median number of children with lead exposure within 5-10 mcg/dL 
SELECT percentile(5_to_10, 0.5) as median FROM children_lead_levels;

-- Found the max number of children with lead_level 5_to_10 grouped by county
SELECT county, MAX(5_to_10) FROM children_lead_levels GROUP BY county ORDER BY MAX(5_to_10)DESC; 

-- Found the max number of children with lead_level 5_to_10 grouped by county
SELECT county, MAX(less_than_5) FROM children_lead_levels GROUP BY county ORDER BY MAX(less_than_5) DESC; 

-- Found the min number of children with lead_level 5_to_10 grouped by county
SELECT county, MIN(5_to_10) FROM children_lead_levels GROUP BY county ORDER BY MIN(5_TO_10); 

-- Found the min number of children with lead_level 5_to_10 grouped by county
SELECT county, MIN(less_than_5) FROM children_lead_levels GROUP BY county ORDER BY MIN(less_than_5); 
