// 1. Create a dataframe by reading raw csv file for Housing Condition from originalDataSets/housing_conditions folder in HDFS.
var housingConRaw = spark.read.format("csv").option("header","true").load("originalDataSets/housing_conditions/housing_con.csv")
housingConRaw.show()
// 2. Drop null value of the dataframe.
housingConRaw = housingConRaw.na.drop
// 3. Select “Funding Cycle”, “County Name”, and “Variable” columns only from the dataframe and save that new dataframe in mutable variable “housingCon”.
var housingCon = housingConRaw.select("Funding Cycle", "County Name", "Variable", "Frequency Count", "Percent of Total Frequency")
// 4. From the previous code result, I noticed I have a weird dataset in our cleaned dataset. Filter them from our dataset.
housingCon = housingCon.filter(!($"Variable".contains("Race:")) && !($"Variable".contains("Hispanic")))
// 5.  Select only rows that we’re interested in
housingCon = housingCon.filter($"Variable" === "Chemical smell indoors" || $"Variable" === "Deteriorated paint inside" || $"Variable" === "Age of building" || $"Variable" === "Deteriorated paint outside")
// 6. make it all upper case for normalization.
housingCon = housingCon.withColumn("County Name", upper(col("County Name")))
housingCon.show()
// 7. save it into the folder “hw8/cleaned_data” folder in HDFS as csv file format.
housingCon.write.format("csv").option("header", true).save("finalCode/housing")



