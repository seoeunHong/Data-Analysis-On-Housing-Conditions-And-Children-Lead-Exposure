
// read in the dataset 
val children_raw_data = spark.read.format("csv").option("header","true").load("originalDataSets/lead_level_test/children_blood_lead_level.csv")

// check if the data was loaded correctly and see all columns 
children_raw_data.show()

// look at relevant columns
var children_data = children_raw_data.select("County", "Year", "Less than 5 mcg/dL", "5-10 mcg/dL")

// look at distinct values in county column 
val distinctCounty = children_data.select("County").distinct
distinctCounty.show(distinctCounty.count.toInt, false)

// look at distinct values in year column
val distinctYear = children_data.select("Year").distinct
distinctYear.show(distinctYear.count.toInt, false)

// look at distinct values in less_than_5 column
val distinctLessThan5 = children_data.select("Less than 5 mcg/dL").distinct
distinctLessThan5.show(distinctLessThan5.count.toInt, false)

// look at distinct values in 5_to_10 column
val distinctLessThan10 = children_data.select("5-10 mcg/dL").distinct
distinctLessThan10.show(distinctLessThan10.count.toInt, false)

