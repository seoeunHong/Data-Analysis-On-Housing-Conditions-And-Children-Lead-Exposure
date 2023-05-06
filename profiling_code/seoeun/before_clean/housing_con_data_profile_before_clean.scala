// 1. Create a dataframe by reading raw csv file for Housing Condition from originalDataSets/housing_conditions folder in HDFS.
var housingConRaw = spark.read.format("csv").option("header","true").load("originalDataSets/housing_conditions/housing_con.csv")
// 2. Check dataframe whether data was loaded correctly by using show() method.
housingConRaw.show()
// 4. Select “Funding Cycle”, “County Name”, and “Variable” columns only from the dataframe and save that new dataframe in mutable variable “housingCon”.
var housingCon = housingConRaw.select("Funding Cycle", "County Name", "Variable", "Frequency Count", "Percent of Total Frequency")
// 5. Select “Variable” column’s distinct (unique) value only and show it 
val distinctVar = housingCon.select("Variable").distinct
distinctVar.show(distinctVar.count.toInt, false)

// 5. Select "Funding Cycle" column’s distinct (unique) value only and show it 
val distinctFC = housingCon.select("Funding Cycle").distinct
distinctFC.show(distinctFC.count.toInt, false)

// 5. Select "County Name"" column’s distinct (unique) value only and show it 
val distinctCN = housingCon.select("County Name").distinct
distinctCN.show(distinctCN.count.toInt, false)