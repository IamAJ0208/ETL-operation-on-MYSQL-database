from mysql.connector import connect,Error
import pandas as pd
import os
import json
import warnings
warnings.filterwarnings('ignore')

def main():
    
    try:
        with open("extraction_load.json",'r') as ext:
            ex=json.load(ext)
        for task in ex["extraction"]:
            ho=task.get("host")
            us=task.get("username")
            pa=task.get("password")
            da=task.get("database")
            file_name=task.get("file_name")   
            table_required=task.get("table_names")
        os.mkdir(file_name)
        conn=connect(
            host=ho,
            username=us,
            password=pa,
            database=da
        )
        print("Connected to database!!")

        project=conn.cursor()
        project.execute('show tables')
        table_names=project.fetchall()
        for i in table_names:
            table=i[0]
            if table not in table_required:
                continue
            query=f'select * from {table}'
            df=pd.read_sql(query,conn)
            df.to_csv(f"{file_name}/{table}.csv",index=False)
        print("Extraction Completed!!!")
    except Error as e:
        print("Connection Failed due to:")
        print(e)
    except Exception as ex:
        print("Extraction Failed due to:")
        print(ex)

if __name__=="__main__":
    print("Extraction Started!")
    main()