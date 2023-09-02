from pyspark.sql import SparkSession
from pyspark.sql.functions import concat,col,lit

import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import json
import os



def concat(source_table,source_column,target_column):
    # source_table[f"{target_column}"]=source_table[source_column].agg(' '.join,axis=1)
    source_table.show()
    # print(source_column[0])
    # print(source_column[1])
    source_table=source_table.withColumn(target_column,concat(col(f"{source_column[0]}"),lit(" "),col(f"{source_column[1]}")))

def to_camelcase(source_table,source_column):
    source_table[f"{source_column}"]=source_table[f"{source_column}"].str.title()
    source_table[f"{source_column}"]=source_table[f"{source_column}"].str.replace(' ','')

def drop(source_table,source_column):
    source_table.drop(columns=[source_column],inplace=True)

def change_format(source_table,source_column,to_format):  
    source_table[f"{source_column}"]=pd.to_datetime(source_table[f"{source_column}"]).dt.strftime(f"{to_format}")

def split(source_table,source_column,target_column):
    source_table[target_column]=source_table[f"{source_column}"].str.split(" ",n=len(target_column)-1,expand=True)
    source_table.drop(columns=[f"{source_column}"],inplace=True)

def join(tables, target_table, on, type):
    dfs = []
    for table in tables:
        df = pd.read_csv(f"{extract_folder_name}/{table}.csv")
        dfs.append(df)

    target_df = dfs[0]
    for i in range(1, len(dfs)):
        target_df = target_df.merge(dfs[i], on=on[i - 1], how=type)
    
    target_df.to_csv(f"{transform_folder_name}/{target_table}.csv", index=False)

def main(config):
    

    try:
        amit=SparkSession.builder.appName("Final_project").getOrCreate()
             
        for i in config["names"]:
            global extract_folder_name
            extract_folder_name=i.get("extract_folder_name")
            global transform_folder_name
            transform_folder_name=i.get("transform_folder_name")
        os.mkdir(transform_folder_name)
        for task in config["transformations"]:
            attr=[]

            for key,value in task.items():
                
                attr.append(value)

            source_table=task.get("source_table")
            operation=task.get("operation")
            attr.remove(task.get("operation"))
            if operation!="join":
                # df=pd.read_csv(f"{extract_folder_name}/{source_table}.csv")
                df=amit.read.csv(f"{extract_folder_name}/{source_table}.csv",header=True,inferSchema=True)
                attr[0]=df
            
            if isinstance(operation, str):
                func=eval(operation)

                func(*attr)


                if operation!="join":
                    df.write.csv(f"{transform_folder_name}/{source_table}.csv",header=True)

            elif isinstance(operation, list):
                for i in operation:
                    oper=[df]
                    
                    for ke,va in i.items():
                        oper.append(ke)
                        op=va

                    func=eval(op)
                    func(*oper)    
                
                df.to_csv(f"{transform_folder_name}/{source_table}.csv",index=False)

        print("Transformation Complete!!")

    except FileExistsError as e:
        print("Transformation Failed due to:")
        print(e)

if __name__=='__main__':
    print("Tranformation Started!!")
    with open("config3.json","r") as f:
        config=json.load(f)
        main(config)