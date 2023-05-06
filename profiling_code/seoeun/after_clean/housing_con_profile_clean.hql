use sh6348_nyu_edu;

CREATE EXTERNAL TABLE housing_conditions
(funding_cycle STRING, county STRING, variable STRING, frequency_count INT, percentage_of_total_frequency DOUBLE) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE location 'hdfs://nyu-dataproc-m/user/sh6348_nyu_edu/finalCode/housing/'
TBLPROPERTIES ("skip.header.line.count"="1");

SELECT DISTINCT funding_cycle FROM housing_conditions;

SELECT funding_cycle, SUM(frequency_count) AS claim_num_by_year 
FROM housing_conditions 
GROUP BY funding_cycle;

SELECT variable, SUM(frequency_count) AS claim_num_by_con
FROM housing_conditions 
GROUP BY variable;

SELECT county, SUM(frequency_count) AS num_dwellings 
FROM housing_conditions 
WHERE county NOT LIKE 'NYC%' AND county != 'ALL COUNTIES' 
GROUP BY county 
ORDER BY num_dwellings DESC;

SELECT county, variable, SUM(frequency_count) AS num_dwellings 
FROM housing_conditions 
WHERE county NOT LIKE 'NYC%' AND county != 'ALL COUNTIES' 
GROUP BY county, variable 
ORDER BY num_dwellings DESC;