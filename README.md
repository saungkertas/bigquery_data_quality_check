## Google BigQuery Data Quality Check

This repository includes tools which help BigQuery data warehouse Operation. 
* Completeness Uniqueness Check 
    + This tool checks completeness and uniqueness per column of a table. the result will be stored to a table in BigQuery. So the user can query the result based on their need.  
    + How to use:  
    `python completeness_uniqueness_check/src/main.py --key <your service account json file> --destination_project_id <BQ project id> --destination_dataset <BQ dataset> --destination_table <BQ table> --date_start <YYYYMMDD> --date_end <YYYYMMDD> --datepref <field partition preference, eg: _PARTITIONTIME>`
