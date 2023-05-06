// load data
val econ_data_raw = spark.read.format("csv").option("header","true").load("originalDataSets/economics_data/econ_data.csv")

// look at dataset
econ_data_raw.show()

// drop null values
econ_data_raw.na.drop

// look at specific columns of dataset
var econ_data = econ_data_raw.select("Area", "Year", "Annual Average Salary                                                                                                                                                  ")

// rename column because of spacing issues                                                                                                                                           
econ_data = econ_data.withColumnRenamed("Annual Average Salary                                                                                                                                                  ", "Annual Average Salary")

// filter data to include rows associated with specific counties in common with other datasets 
econ_data= econ_data.filter($"Area".contains("Albany County") || $"Area".contains("Clinton County") || $"Area".contains("Tompkins County") || $"Area".contains("Rockland County") || $"Area".contains("Tioga County") || $"Area".contains("Orange County") || $"Area".contains("Westchester County") || $"Area".contains("Broome County") || $"Area".contains("Niagara County") || $"Area".contains("Cayuga County") || $"Area".contains("Monroe County") || $"Area".contains("Schenectady County") || $"Area".contains("Cortland County") || $"Area".contains("Rensselaer County") || $"Area".contains("Onondaga County") || $"Area".contains("Erie County") || $"Area".contains("Columbia County") || $"Area".contains("Oneida County"))

// only include the county name and not the word county 
econ_data= econ_data.withColumn("Area", upper(substring_index(col("Area"), " ", 1)))

// filter data based on year 
val final_data = econ_data.filter(col("Year") >= 2006 && col("Year") <=2019)

final_data.show()

// write new dataset 
final_data.write.format("csv").option("header", true).save("finalCode/econ")