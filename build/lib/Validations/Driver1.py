
import json
import csv
import pandas as pd
import numpy as np
import hashlib
from datetime import datetime
import configparser
import asyncio
from Validations.CsvParser import getDFfromCsv, getDFfromXlsxMerge, getDFfromXls, check_dtype, check_ruleValidation,getDFfromXlsx
from Validations.JsonParser import GetAllValueByKey, GetRules
from Validations.Utility import getUniqueValueList, list_contains
from Validations.SQLDriver import GetSQLDF,GetMSSQLDF,ExecuteSQLQuery

import nest_asyncio
def main(configFilePath):


# def add_group_column(df, batch_size=500):
#         group = ['g' + str(i // batch_size + 1) for i in range(len(df))]
#         df['group'] = group
#         print('Group ---->>',df)
#         return df
 def add_group_column(df, batch_size=10000):
    group = ['g' + str(i // batch_size + 1) for i in range(len(df))]
    df['group'] = group
    return df


# def validation(sourcedf, destinationdf, batch_id):
#             # Filter data by the specified batch_id
#             source_batch = sourcedf[sourcedf['group'] == batch_id]
#             destination_batch = destinationdf[destinationdf['group'] == batch_id]

#             # Compare the dataframes and return the result in a new dataframe
#             compare_result = pd.concat([source_batch[~source_batch.isin(destination_batch)], destination_batch[~destination_batch.isin(source_batch)]])
#             return compare_result

 async def validation(sourcedf, destinationdf, batch_id):
    # Filter data by the specified batch_id
    source_batch = sourcedf[sourcedf['group'] == batch_id]
    destination_batch = destinationdf[destinationdf['group'] == batch_id]

    # Compare the dataframes and return the result in a new dataframe
    compare_result = pd.concat([source_batch[~source_batch.isin(destination_batch)], destination_batch[~destination_batch.isin(source_batch)]])
    return compare_result

# async def validation(sourcedf, destinationdf, batch_id):
#     # Merge dataframes based on the 'UserID' column
#     merged_df = pd.merge(sourcedf, destinationdf, on='UserID', suffixes=('_source', '_destination'), how='outer')

#     # Find rows where the data doesn't match between the two dataframes
#     compare_result = merged_df[
#         (merged_df['group_source'] != merged_df['group_destination'])
#         | (merged_df['group_source'].isnull() | merged_df['group_destination'].isnull())
#     ]

#     # Extract only the columns you need from the comparison result
#     compare_result = compare_result[['UserID', 'group_source', 'group_destination']]

#     return compare_result

 

 async def DoubleDataValidation():
        
        #configFilePath="configuration.ini"
        parser = configparser.ConfigParser()
        parser.read(configFilePath)
        
        SOURCE_TYPE = parser.get('APP', 'source_type')
        DESTINATION_TYPE = parser.get('APP', 'dest_type')
        
        subject = 'Data_Validation'
        date = datetime.now().strftime("%Y%m%d_%I%M%S")        
        output_file_path = parser.get('APP', 'output_file')
        reportOutputDir = output_file_path + '/report/'
        errorOutputDir = output_file_path + '/error/'
        
        sourcedf = pd.DataFrame()

        ############ Read Source DataFile #######

        ### CSV ####

        if SOURCE_TYPE == 'CSV':
            source_data_file_path = parser.get('SOURCE', 'source_data_file_path')
            skip_rows = parser.get('SOURCE', 'SKIP_ROWS')
            sourcedf = getDFfromCsv(source_data_file_path,skip_rows) 
            source_row, no_of_columns = sourcedf.shape  

        ### XLSX ####

        if SOURCE_TYPE == 'XLSX':
            source_data_file_path = parser.get('SOURCE', 'source_data_file_path')
            sheet_name = parser.get('SOURCE', 'sheet_name')
            skip_rows = parser.get('SOURCE', 'SKIP_ROWS')
            sourcedf = getDFfromXlsx(source_data_file_path, sheet_name, "", "", skip_rows)
            source_row, no_of_columns = sourcedf.shape

        ### MSSql ####
        if SOURCE_TYPE == 'MSSQL':
            server = parser.get('SOURCE', 'source_server')
            User = parser.get('SOURCE', 'source_user')
            Password = parser.get('SOURCE', 'source_password')
            Database = parser.get('SOURCE', 'source_database')
            SOURCE_SCHEMA_NAME = parser.get('vTurbineMasterData_Source', 'schema_name_source')
            SOURCE_TABLE_NAME = parser.get('vTurbineMasterData_Source' , 'source_query_filter')
            sourcedf = GetSQLDF(server, SOURCE_TABLE_NAME, User, Password, Database)
            source_row, no_of_columns = sourcedf.shape
            print("Source Rows =",source_row)

        ### MySql ####
        if SOURCE_TYPE == 'MYSQL':
            User = parser.get('SOURCE', 'source_user')
            Password = parser.get('SOURCE', 'source_password')
            Database = parser.get('SOURCE', 'source_database')
            SOURCE_SCHEMA_NAME = parser.get('vTurbineMasterData_Source' , 'schema_name_source')
            SOURCE_TABLE_NAME = parser.get('vTurbineMasterData_Source' , 'source_query_filter')
            task1 = asyncio.create_task(GetSQLDF(SOURCE_TABLE_NAME, User, Password, Database))
            sourcedf = await task1
            source_row, no_of_columns = sourcedf.shape
            # group = ['g' + str(i) for i in range(1, len(sourcedf) + 1)]
            # sourcedf['group'] = group
            print("No_of_rows_MYSQL SourceDF => ", source_row)


        ############## Read Destination DataFile ####

        ### CSV ####

        destinationdf = pd.DataFrame()

        if DESTINATION_TYPE == 'CSV':
            destination_data_file_path = parser.get('DEST', 'dest_data_file_path')
            skip_rows = parser.get('DEST', 'SKIP_ROWS')
            destinationdf = getDFfromCsv(destination_data_file_path, skip_rows) 
            destination_rows, no_of_columns = destinationdf.shape  

        ### XLSX ####

        if DESTINATION_TYPE == 'XLSX':
            destination_data_file_path = parser.get('DEST', 'dest_data_file_path')
            sheet_name = parser.get('DEST', 'sheet_name')
            skip_rows = parser.get('DEST', 'SKIP_ROWS')
            destinationdf = getDFfromXlsx(destination_data_file_path, sheet_name, "", "", skip_rows)
            destination_rows, no_of_columns = sourcedf.shape

        ### MSSQLDF ####

        if DESTINATION_TYPE == 'MSSQL':
            User = parser.get('DEST', 'dest_user') 
            Password = parser.get('DEST', 'dest_password')
            Database = parser.get('DEST', 'dest_database')
            DESTINATION_SCHEMA_NAME = parser.get('vTurbineMasterData_Dest', 'schema_name_dest')
            DESTINATION_TABLE_NAME = parser.get('vTurbineMasterData_Dest' , 'destination_query_filter')
            task2 = asyncio.create_task(GetMSSQLDF(DESTINATION_TABLE_NAME, User, Password, Database))
            destinationdf = await task2
            # group = ['g' + str(i) for i in range(1, len(destinationdf) + 1)]
            # destinationdf['group'] = group
            destination_rows, no_of_columns = destinationdf.shape
            print("No_of_rows_MSSQL Destination => ", destination_rows)

            

        ### MYSQL####

        if DESTINATION_TYPE == 'MYSQL':
            User = parser.get('DEST', 'dest_user')
            Password = parser.get('DEST', 'dest_password')
            Database = parser.get('DEST', 'dest_database')
            DESTINATION_SCHEMA_NAME = parser.get('vTurbineMasterData_Dest', 'schema_name_dest')
            DESTINATION_TABLE_NAME = parser.get('vTurbineMasterData_Dest' , 'destination_query_filter')
            destinationdf = GetMSSQLDF(DESTINATION_TABLE_NAME, User, Password, Database)
            destination_rows, no_of_columns = destinationdf.shape

        # ***************** Validation **********************
    

        if source_row == destination_rows:
            print("Validations Process is Running!!!")

            # Add 'group' column to dataframes if not already present
            if 'group' not in sourcedf.columns:
                sourcedf = add_group_column(sourcedf)

            if 'group' not in destinationdf.columns:
                destinationdf = add_group_column(destinationdf)

            batch_ids = list(sourcedf['group'].unique())
            # print("Batch IDs:", batch_ids)

            # Group dataframes by 'group' column
            sourcedf_grouped = sourcedf.groupby('group')
            destinationdf_grouped = destinationdf.groupby('group')
            # print("Size of sourcedf_grouped:", len(sourcedf_grouped))
            # print("Size of destinationdf_grouped:", len(destinationdf_grouped))
            validation_results = []
            for batch_id in batch_ids:
              result = await validation(sourcedf_grouped.get_group(batch_id),
                            destinationdf_grouped.get_group(batch_id),
                            batch_id)
            validation_results.append(result)

            # Wait for all tasks to complete



 
        

    # ... Rest of your code ...


        #     # Create a list to store batch IDs
        #     batch_ids = list(sourcedf['group'].unique())

        # # Your comparison logic...
        #     # compare_hash = pd.concat([sourcedf[~sourcedf.hash.isin(destinationdf.hash)], destinationdf[~destinationdf.hash.isin(sourcedf.hash)]])
        #     # compare = sourcedf.compare(destinationdf)

        #     validation_results = {}
        #     for batch_id in batch_ids:
        #             result = validation(sourcedf, destinationdf, batch_id)
        #             validation_results[batch_id] = result
        #     #  Print comparison results for each batch
        #     for batch_id, result in validation_results.items():
        #         print(f"Comparison Result for Batch '{batch_id}':")

       

        # Initialize a list to store results for each batch
             

        compare_hash = pd.concat([sourcedf[~sourcedf.hash.isin(destinationdf.hash)], destinationdf[~destinationdf.hash.isin(sourcedf.hash)]])
        compare = sourcedf.compare(destinationdf)
   
            
        get_index = sourcedf.index[sourcedf.ne(destinationdf).any(axis=1)]

        source_error_df = sourcedf[sourcedf.index.isin(get_index)].sort_index() 
        source_error_df = source_error_df.loc[:, source_error_df.columns != 'hash']

        destination_error_df = destinationdf[destinationdf.index.isin(get_index)].sort_index() 
        destination_error_df = destination_error_df.loc[:, destination_error_df.columns != 'hash']
            
        print("Error Source Dataframe : \n", source_error_df)
        print("Error Destination Dataframe : \n", destination_error_df)

        sample_df_source = sourcedf.sample(frac=0.1).sort_index()
        sample_index = sample_df_source.index 
        sample_df_destination = destinationdf[destinationdf.index.isin(sample_index)].sort_index()

        print("Source Random Record : \n", sample_df_source) 
        print("Destination Random Record : \n", sample_df_destination)

        sample_df_source_row, no_of_columns = sample_df_source.shape
        sample_df_destination_row, no_of_columns = sample_df_destination.shape

        if sample_df_source_row == sample_df_destination_row:
                sample_df_source = sample_df_source.loc[:, sample_df_source.columns != 'hash']
                sample_df_destination = sample_df_destination.loc[:, sample_df_destination.columns != 'hash']
                compare = sample_df_source.compare(sample_df_destination, keep_shape=True, keep_equal=False)
                print("Compare of Random Data Result : \n", compare)
        else:
                print("COUNT NOT MATCHED")
                print("COUNT OF SOURCE DATAFRAME : ", sample_df_source_row)
                print("COUNT OF DESTINATION DATAFRAME : ", sample_df_source_row)

        try:
                col_name_min = parser.get('SOURCE', 'col_name_min')
                if sourcedf[col_name_min].min() == destinationdf[col_name_min].min():
                    min = "MIN VALUE MATCH"
                    print(min)
                else:
                    min = "MIN VALUE NOT MATCH"
                    print(min)
        except:  
                min = "Column Name Not Match For Min Condition"
                print(min)      

        try:
                col_name_max = parser.get('SOURCE', 'col_name_max')
                if sourcedf[col_name_max].max() == destinationdf[col_name_max].max():
                    max = "MAX VALUE MATCH"
                    print(max)
                else:
                    max = "MAX VALUE NOT MATCH"
                    print(max)
        except: 
                max = "Column Name Not Match For Max Condition"
                print(max)

        try:
                col_name_sum = parser.get('SOURCE', 'col_name_sum')  
                if sourcedf[col_name_sum].astype(int).sum() == destinationdf[col_name_sum].astype(int).sum():
                    sum = "SUM VALUE MATCH"
                    print(sum)
                else:
                    sum = "SUM VALUE NOT MATCH"
                    print(sum)
        except: 
                sum = "Column Name Not Match For Sum Condition"
                print(sum)

        try:        
                col_name_avg = parser.get('SOURCE', 'col_name_avg')  
                if sourcedf[col_name_avg].astype(int).mean() == destinationdf[col_name_avg].astype(int).mean():
                    avg = "Average VALUE MATCH"
                    print(avg)
                else:
                    avg = "Average VALUE NOT MATCH"
                    print(avg)
        except: 
                avg = "Column Name Not Match For Average Condition"        
                print(avg)

                fileName = "Report_" + subject + "_" + date + ".csv"
                source_error_df.to_csv(reportOutputDir + fileName, index=False)
                destination_error_df.to_csv(reportOutputDir + fileName, index=False)

                fileName = "Report_" + subject + "_" + date + ".html" 
                html1 = source_error_df.to_html()
                html2 = destination_error_df.to_html()
                html3 = compare.to_html()
                html = f"<html><body><h4>Source DF Value Not Match : </h4><br><table>{html1}</table><br><h4>Destination DF Value Not Match : </h4><br><table>{html2}</table><br><h4>Compare of Random Data(10%) Result : </h4><br><table>{html3}</table><br><h4>Validations : </h4><h4>1 - {min}</h4><h4>2 - {max}</h4><h4>3 - {sum}</h4><h4>4 - {avg}</h4></body></html>"
                filePath = reportOutputDir
                text_file = open(filePath + fileName, "w")
                text_file.write(html)
                text_file.close()
        else:
                print("DataFrame Row Count Not Match")
                print("COUNT OF SOURCE DATAFRAME : ", source_row)
                print("COUNT OF DESTINATION DATAFRAME : ", destination_rows)
 nest_asyncio.apply()  # Enable nested asyncio

 asyncio.run(DoubleDataValidation())

#main()






