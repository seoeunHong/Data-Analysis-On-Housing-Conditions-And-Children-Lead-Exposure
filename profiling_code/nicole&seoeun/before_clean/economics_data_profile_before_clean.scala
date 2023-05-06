// load data
val econ_data_raw = spark.read.format("csv").option("header","true").load("originalDataSets/economics_data/econ_data.csv")

// look at dataset
econ_data_raw.show()

// look at specific columns of dataset
var econ_data = econ_data_raw.select("Area", "Year", "Annual Average Salary                                                                                                                                                  ")

// rename column because of spacing issues                                                                                                                                           
econ_data = econ_data.withColumnRenamed("Annual Average Salary                                                                                                                                                  ", "Annual Average Salary")

// look at distinct values in Area column 
val distinctArea = econ_data.select("Area").distinct
distinctArea.show(distinctArea.count.toInt, false)

// look at distinct values in Year column 
val distinctYear = econ_data.select("Year").distinct
distinctYear.show(distinctYear.count.toInt, false)
