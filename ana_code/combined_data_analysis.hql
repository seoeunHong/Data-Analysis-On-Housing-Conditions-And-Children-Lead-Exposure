-- Find out the total number of children with lead levels 5_to_10 range per county 
use sh6348_nyu_edu;

SELECT county, SUM(5_to_10) AS num_of_children 
FROM selected_children_lead_levels 
GROUP BY county 
ORDER BY num_of_children DESC;

DROP TABLE IF EXISTS num_of_children_by_county;
CREATE TABLE num_of_children_by_county 
AS (
    SELECT county, SUM(5_to_10) AS num_of_children 
    FROM selected_children_lead_levels 
    GROUP BY county 
    ORDER BY num_of_children DESC
    );

-- created table to include individual counties and the sum of housing per county
-- this will tell us a little more about the total number of housing this is data is for 
DROP TABLE IF EXISTS num_dwellings_by_county;
CREATE TABLE num_dwellings_by_county 
AS (
    SELECT county, SUM(frequency_count) AS num_dwellings 
    FROM housing_conditions 
    WHERE county NOT LIKE 'NYC%' AND county != 'ALL COUNTIES' 
    GROUP BY county 
    ORDER BY num_dwellings DESC
    );

-- joins both tables based on county to show us the relationship between number of houses with paint complaints and number of children with 5_to_10 range of blood lead levels
DROP TABLE IF EXISTS joined_county;
CREATE TABLE joined_county AS (SELECT c.county, c.num_of_children, d.num_dwellings 
FROM num_of_children_by_county AS c 
INNER JOIN num_dwellings_by_county AS d 
ON c.county = d.county);

-- view the rows of this joined table 
SELECT * 
FROM joined_county;

-- view the rows of this joined table ordered by number of dwellings/houses
SELECT * 
FROM joined_county 
ORDER BY num_dwellings;

-- view the rows of this joined table ordered by number of children with lead levels in 5 to 10 range
SELECT * 
FROM joined_county 
ORDER BY num_of_children;

-- find the correlation between number of children and number of houses/dwellings 
SELECT corr(num_of_children, num_dwellings) AS correlation 
FROM joined_county;

-- finding out the number of children who have less than 5 lead levels per county 
SELECT county, SUM(less_than_5) AS num_of_children 
FROM selected_children_lead_levels 
GROUP BY county 
ORDER BY num_of_children DESC;

-- then we created a table from that which we joined the table that has the sum of dwelling per county 
DROP TABLE IF EXISTS num_of_less_lead_children;
CREATE TABLE num_of_less_lead_children 
AS (
    SELECT county, SUM(less_than_5) AS num_of_children 
    FROM selected_children_lead_levels 
    GROUP BY county 
    ORDER BY num_of_children DESC
    );

DROP TABLE IF EXISTS joined_county_less_lead;
CREATE TABLE joined_county_less_lead 
AS (
    SELECT c.county, c.num_of_children, d.num_dwellings 
    FROM num_of_less_lead_children AS c 
    INNER JOIN num_dwellings_by_county AS d 
    ON c.county = d.county
    );

-- view all the rows of this joined table 
SELECT * 
FROM joined_county_less_lead;

-- we realized that we it will be more accurate to compare percentage than numbers because each county
-- has a different population count 
DROP TABLE IF EXISTS relative_percentage;
CREATE TABLE relative_percentage 
AS (
    SELECT county, ROUND(SUM(less_than_5)*100 / (SUM(less_than_5) + SUM(5_to_10)), 2) AS less_than_5, 
    ROUND(SUM(5_to_10)*100/ (SUM(less_than_5) + SUM(5_to_10)), 2) AS 5_to_10 
    FROM selected_children_lead_levels 
    GROUP BY county
    );

DROP TABLE IF EXISTS joined_county_percentage;
CREATE TABLE joined_county_percentage 
AS (
    SELECT r.county, r.less_than_5, r.5_to_10, d.num_dwellings 
    FROM relative_percentage AS r 
    INNER JOIN num_dwellings_by_county AS d 
    ON r.county = d.county
    );

-- View all the rows of this table 
SELECT * 
FROM joined_county_percentage;

-- view all the rows of this table ordered by 5_to_10 range column 
SELECT * 
ROM joined_county_percentage 
ORDER BY 5_to_10 DESC;

-- view all the rows of this table ordered by num_dwellings column 
SELECT * 
FROM joined_county_percentage 
ORDER BY num_dwellings DESC;

-- view all the rows ordered by less than 5 county 
SELECT * 
FROM joined_county_percentage 
ORDER BY less_than_5 DESC;

-- finding the average of the annula average salary per county and create a table from that 
SELECT area, ROUND(AVG(annual_average_salary), 2) AS avg_econ_status 
FROM econ_data 
GROUP BY area;

DROP TABLE IF EXISTS county_econ;
CREATE TABLE county_econ 
AS (
    SELECT area, ROUND(AVG(annual_average_salary), 2) AS avg_econ_status 
    FROM econ_data GROUP BY area
    );

-- join this with the other table that includes children blood lead level data and number of dwellings
DROP TABLE IF EXISTS county_econ_percentage;
CREATE TABLE county_econ_percentage 
AS (
    SELECT j.county, j.less_than_5, j.5_to_10, j.num_dwellings, e.avg_econ_status 
    FROM joined_county_percentage AS j 
    INNER JOIN county_econ AS e 
    ON j.county = e.area
    );

-- view all the rows from this table ordered by average_econ_status 
SELECT * 
FROM county_econ_percentage 
ORDER BY avg_econ_status;

-- view all the rows from this table ordered by 5_to_10 column
SELECT * 
FROM county_econ_percentage 
ORDER BY 5_to_10 DESC;

SELECT county, 
    less_than_5, 
    ROW_NUMBER() OVER (ORDER BY less_than_5 DESC) AS less_than_5_rank,
    5_to_10,
    ROW_NUMBER() OVER (ORDER BY 5_to_10 DESC) AS 5_to_10_rank,
    num_dwellings,
    ROW_NUMBER() OVER (ORDER BY num_dwellings DESC) AS num_dwellings_rank,
    avg_econ_status,
    ROW_NUMBER() OVER (ORDER BY avg_econ_status DESC) AS avg_econ_status_rank
FROM county_econ_percentage;

SELECT county, less_than_5 
FROM county_econ_percentage 
ORDER BY less_than_5 ASC 
LIMIT 1;

SELECT county, less_than_5 
FROM county_econ_percentage 
ORDER BY less_than_5 DESC 
LIMIT 1;

SELECT county, 5_to_10
FROM county_econ_percentage 
ORDER BY 5_to_10 ASC 
LIMIT 1;

SELECT county, 5_to_10
FROM county_econ_percentage 
ORDER BY 5_to_10 DESC 
LIMIT 1;

SELECT county, avg_econ_status
FROM county_econ_percentage 
ORDER BY avg_econ_status ASC 
LIMIT 1;

SELECT county, avg_econ_status
FROM county_econ_percentage 
ORDER BY avg_econ_status DESC 
LIMIT 1;

SELECT county, num_dwellings
FROM county_econ_percentage 
ORDER BY num_dwellings DESC 
LIMIT 1;

SELECT county, num_dwellings
FROM county_econ_percentage 
ORDER BY num_dwellings  LIMIT 1;

SELECT STDDEV(less_than_5) AS less_than_5_stdev
FROM county_econ_percentage;

SELECT STDDEV(5_to_10) AS 5_to_10_stdev
FROM county_econ_percentage;

SELECT STDDEV(avg_econ_status) AS avg_econ_status_stdev
FROM county_econ_percentage;

SELECT STDDEV(num_dwellings) AS num_dwellings_stdev
FROM county_econ_percentage;

SELECT CORR(avg_econ_status, less_than_5) AS relationship_econ_less_than_5
FROM county_econ_percentage;

SELECT CORR(avg_econ_status, 5_to_10) AS relationship_econ_5_to_10
FROM county_econ_percentage;

SELECT CORR(avg_econ_status, num_dwellings) AS relationship_econ_num_dwellings
FROM county_econ_percentage;

SELECT CORR(less_than_5, num_dwellings) AS relationship_less_than_5_num_dwellings
FROM county_econ_percentage;

SELECT CORR(5_to_10, num_dwellings) AS relationship_5_to_10_num_dwellings
FROM county_econ_percentage;