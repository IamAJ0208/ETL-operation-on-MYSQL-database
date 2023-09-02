from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_utils import database_exists,create_database,drop_database
import pandas as pd
import os
import json

def main():
    try:
        with open("extraction_load.json",'r') as ext:
            ex=json.load(ext)
        for task in ex["loading"]:
            ho=task.get("host")
            us=task.get("username")
            pa=task.get("password")
            port=task.get("port")
            database_name=task.get("database_name")
            transform_folder_name=task.get("transform_folder_name")
        engine=create_engine(f"mysql+mysqlconnector://{us}:{pa}@{ho}:{port}/{database_name}")
        if database_exists(engine.url):
            drop_database(engine.url)
        create_database(engine.url)
            
        transformed_files=os.listdir(transform_folder_name)

        for i in transformed_files:
            table=i.replace(".csv","")
            df=pd.read_csv(f"{transform_folder_name}/{i}")
            df.to_sql(f"{table}",con=engine,index=False)
        print("Loading Completed!!!")

    except SQLAlchemyError as s:
        print(f"Connection failed because of {s}")
    except Exception as ex:
        print("Loading Failed due to:")
        print(ex)


if __name__=="__main__":
    print("Loading Started!")
    main()