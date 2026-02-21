Data Engineering Technique:
I have applied 3 Layer Layer architechture amd created 1 notebook with the generic function
Bronze - Maintains the data as is from the source
Silver - Data pre-processing layer (Data Munging,Data Enrichment,Data Curation, Data Customization) 
Gold - Reporting(Joins, Window, aggregation and advanced analytics(lead,lag,rollup,cube))

Deployment Strategy:
I used to upload the code to gitHub at the end of the day till development completes.  
Prod deployment will be done manually by getting the code from the git and deploy to prod.

The below function not converting the space to underscore for column_names

for col_name in silver_df.columns:
    if len(col_name.split()) > 1:
        new_col_name = '_'.join(col_name.split())
        silver_df1 = silver_df.withColumnRenamed(col_name, new_col_name)
        
silver_df1.show()