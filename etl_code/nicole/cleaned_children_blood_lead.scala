
// read in the dataset 
val children_raw_data = spark.read.format("csv").option("header","true").load("originalDataSets/lead_level_test/children_blood_lead_level.csv")

// check if the data was loaded correctly and see all columns 
children_raw_data.show()

// drop null values of dataframe 
children_raw_data.na.drop

// select relevant columns only and create new dataframe with this 
var children_data = children_raw_data.select("County", "Year", "Less than 5 mcg/dL", "5-10 mcg/dL")

// filter data by year to only include data from year between 2006 and 2019

children_data = children_data.filter(col("Year") >= 2006 && col("Year") <=2019)

// Creates a new dataframe dfUpper by transforming the values in the  “county” column to uppercase.
val dfUpper = children_data.withColumn("County", upper(col("County")))

// This code creates a new DataFrame with a binary column called hasHighBloodLead which indicates whether children in a given county have high blood lead levels based on the count of children whose blood lead level fall within the range of 5-10 mg/L. 
val dfWithBinaryColumn = dfUpper.withColumn("hasHighBloodLead", when(col("5-10 mcg/dL") > 0, 1).otherwise(0))

// show this new dataframe with new column
dfWithBinaryColumn.show()

// write cleaned data set 
dfWithBinaryColumn.write.format("csv").option("header", true).save("finalCode/lead")

