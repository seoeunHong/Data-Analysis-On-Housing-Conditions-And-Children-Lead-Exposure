# Exploring the Relationship Between Housing Environment and Children's Lead Exposure

If you want to read more details about this project, check this [personal blog post](https://seoeunhong.github.io/posts/Analysis-on-Housing-Environment-And-Children's-Lead-Exposure/) or this [Medium blog post](https://medium.com/@sun-hong/nyu-big-data-team-project-exploring-the-relationship-between-housing-environment-and-childrens-a23f3d6f79f7).

## 1. File Directories and Files

```

├── ana_code
│   ├── combined_data_analysis.hql
│		├── screenshots
│   │   ├── **/*.png
├── data
│ 	├── raw
│   │   ├── **/*.csv
├── data_ingest
│   ├── data_ingestion.sh
│   ├── data_ingest_screenshot.png
├── etl_code
│   ├── nicole
│   │   ├── cleaned_children_blood_lead.scala
│   │   ├── **/*.png
│		├── seoeun
│   │   ├── cleaned_housing_con.scala
│   │   ├── **/*.png
│		├── nicole&seoeun
│   │   ├── cleaned_economic.scala
│   │   ├── **/*.png
├── profiling_code
│   ├── nicole
│   │   ├── before_clean
│   │   │   ├── children_blood_lead_profile_before_clean.scala
│   │   │   ├── **/*.png
│   │   ├── after_clean
│   │   │   ├── children_blood_lead_profile_clean.hql
│   │   │   ├── **/*.png
│		├── seoeun
│   │   ├── before_clean
│   │   │   ├── housing_con_data_profile_before_clean.scala
│   │   │   ├── **/*.png
│   │   ├── after_clean
│   │   │   ├── housing_con_profile_clean.hql
│   │   │   ├── **/*.png
│		├── nicole&seoeun
│   │   ├── before_clean
│   │   │   ├── economics_data_profile_before_clean.scala
│   │   │   ├── **/*.png
│   │   ├── after_clean
│   │   │   ├── econ_profile_clean.hql
│   │   │   ├── **/*.png
└── README.md
```

## 2. How to Build Our Code and Run Our Code

### a. Data Ingestion

1. Start dataproc

2. Upload the original version of the csv datasets which can be found in `data/raw`  to dataproc

3. Upload the script file `data_ingestion.sh` which can be found in `data_ingest` to dataproc

4. Make executable file and run it with this commands

   ```bash
   chmod +x data_ingestion.sh
   ./data_ingestion.sh
   ```

   this will set up a folder called `originalDataSets` all your csv files on hdfs

### b. Profiling before cleaning datasets

1. Upload `children_blood_lead_profile_before_clean.scala` located in `profiling_code/nicole/before_clean` to dataproc

2. Run that scala file using this command

   ```bash
   spark-shell --deploy-mode client -i children_blood_lead_profile_before_clean.scala
   ```

3. After running this command, you will see the results of this profile on the spark Scala shell

4. Keep repeating `1 - 3` steps for scala files `housing_con_data_profile_before_clean.scala` and `economics_data_profile_before_clean.scala` located in `profiling_code/seoeun/before_clean` and `profiling_code/nicole&seoeun/before_clean`

### c. Cleaning datasets

1. Upload `cleaned_children_blood_lead.scala` located in `etl_code/nicole` to dataproc

2. Run that scala file using this command

   ```bash
   spark-shell --deploy-mode client -i cleaned_children_blood_lead.scala
   ```

3. After running this command, you will see the results at `finalCode/lead` on your hdfs. Note that results for other scala files will be located in `finalCode/housing` and `finalCode/econ`

4. Keep repeating `1 - 3` steps for scala files `cleaned_housing_con.scala` and `cleaned_economic.scala` located in `etl_code/seoeun` and `etl_code/nicole&seoeun`

### d.  Profiling After cleaning datasets

1. Upload `children_blood_lead_profile_clean.hql` located in `profiling_code/nicole/after_clean` to dataproc

2. Run that HiveQL file using this command

   ```bash
   beeline -u "jdbc:hive2://localhost:10000" -f children_blood_lead_profile_clean.hql
   ```

3. After running this command, you will see the results of this profile on the hive shell

4. Keep repeating `1 - 3` steps for hiveQL files `housing_con_profile_clean.hql` and `econ_profile_clean.hql` located in `profiling_code/seoeun/after_clean` and `profiling_code/nicole&seoeun/after_clean`

### e. Analysis

1. Upload `combined_data_analysis.hql` located in `ana_code` to dataproc

2. Run that HiveQL file using this command

   ```bash
   beeline -u "jdbc:hive2://localhost:10000" -f combined_data_analysis.hql
   ```

3. After running this command, you will see the results of this profile on the hive shell
