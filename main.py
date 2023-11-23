import duckdb
import pandas as pd

csv_path = "data/Electric_Vehicle_Population_Data.csv"
output_dir = "output/"

con = duckdb.connect(database=':memory:', read_only=False)

def insert():
    print("start insert")
    con.sql(f"CREATE TABLE electrocars AS SELECT * FROM '{csv_path}'")
    print("end insert")

def show():
    con.sql(f"SELECT * FROM electrocars").show()
    print("-------------------------------------------------------")
    con.sql(f"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'electrocars'").show()
    
def store_to_parquet(inner_sql, partition_by, partition_folder_or_file_name):
    partition_sql = ""
    if partition_by:
        partition_sql = "PARTITION_BY " + partition_by + ","
    else:
        partition_folder_or_file_name += ".parquet"
        
    full_sql = f"""COPY 
            ({inner_sql}) 
            TO '{output_dir}{partition_folder_or_file_name}' 
            (FORMAT PARQUET, 
            {partition_sql} 
            OVERWRITE_OR_IGNORE 1);
            """
            
    print(("==========================full_sql============================="))
    print(full_sql)
    print(("==========================full_sql============================="))
            
    con.sql(full_sql)
    

def count_e_cars_per_city():
    query = """SELECT city, COUNT(*) AS count, "Model Year"
                FROM electrocars 
                GROUP BY city, "Model Year"
                ORDER BY count desc
                """
    
    store_to_parquet(query, '"Model Year"', 'e_cars_per_city')
    
def top_cars(count = 3):
    query = """
    WITH RankedCars AS (
    SELECT
        "Model Year",
        model,
        COUNT(*) AS car_count,
        RANK() OVER (PARTITION BY "Model Year" ORDER BY COUNT(*) DESC) AS rnk
    FROM electrocars
    GROUP BY "Model Year", model
    )
    SELECT
        "Model Year",
        model,
        car_count
    FROM RankedCars
    WHERE rnk <= 3"""
    
    store_to_parquet(query, '"Model Year"', 'top_3_cars')
    
def top_in_each_postal_code():
    query = """
        WITH RankedCars AS (
    SELECT
        "Model Year",
        "Postal Code",
        COUNT(*) AS car_count,
        RANK() OVER (PARTITION BY "Model Year" ORDER BY COUNT(*) DESC) AS rnk
    FROM electrocars
    GROUP BY "Model Year", "Postal Code"
    )
    SELECT
        "Model Year",
        "Postal Code",
        car_count
    FROM RankedCars
    """
    con.sql(query).show()
    store_to_parquet(query, '"Model Year"', 'top_in_each_postal_code')
    
def cars_per_year():
    query = """
        SELECT "Model Year", COUNT(*) AS car_count
        FROM electrocars
        GROUP BY "Model Year"
        ORDER BY car_count DESC
    """
    store_to_parquet(query, '"Model Year"', 'cars_per_year')
    store_to_parquet(query, None, 'cars_per_year_single_file')

def main():
    print("===============================================================")
    insert()
    print("===============================================================")
    show()
    print("===============================================================")
    count_e_cars_per_city()
    print("===============================================================")
    top_cars()
    print("===============================================================")
    top_in_each_postal_code()
    print("===============================================================")
    cars_per_year()
    print("===============================================================")
    
    con.close()
    print("======================CONNECTION CLOSED========================")
   


if __name__ == "__main__":
    main()
