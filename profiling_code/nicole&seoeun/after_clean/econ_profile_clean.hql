use sh6348_nyu_edu;

-- create hive table
CREATE EXTERNAL TABLE econ_data
(area STRING, year INT, annual_average_salary INT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE location 'hdfs://nyu-dataproc-m/user/sh6348_nyu_edu/finalCode2/econ' TBLPROPERTIES ("skip.header.line.count"="1");

-- obtain metadata of this table
DESCRIBE econ_data;

-- find distinct values of area after cleaning 
SELECT DISTINCT area FROM econ_data;

-- find distinct values of year after cleaning 
SELECT DISTINCT year FROM econ_data;

-- find distinct values of annual_average_salary
SELECT DISTINCT annual_average_salary FROM econ_data;

-- count the number of counties in the area column 
SELECT COUNT(area) FROM econ_data;

-- count the number of year in year column
SELECT COUNT(year) FROM econ_data;

-- count the number of annual_average_salary in year column
SELECT COUNT(annual_average_salary) FROM econ_data;

-- Find the mean number of annual_average_salary
SELECT AVG(annual_average_salary) FROM econ_data;

-- Find the mode number of annual_average_salary
SELECT annual_average_salary, COUNT(*) as mode FROM econ_data GROUP BY annual_average_salary ORDER BY mode DESC LIMIT 1;

-- find the median annual_average_salary
SELECT percentile(annual_average_salary, 0.5) as median FROM econ_data;

-- Find the maximum average salary per county 
SELECT area, MAX(annual_average_salary) FROM econ_data GROUP BY area ORDER BY MAX(annual_average_salary) DESC;

-- Find the mininum average salary per county 
SELECT area, MIN(annual_average_salary) FROM econ_data GROUP BY area ORDER BY MIN(annual_average_salary);

