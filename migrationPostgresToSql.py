import pandas as pd
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine
from datetime import datetime
import psycopg2

mysql_dbConnect={"host":"localhost","port":"3306","user":"root","password":"Sruthi@1809","database":"dvdrentals"}
postgres_dbConnect={"host":"localhost","port":"5432","user":"postgres","password":"Sruthi@1809","database":"project"}
def getData():
    query="""SELECT
        p.payment_id,
        p.amount,
        p.payment_date,
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email
    FROM
        payment p
    INNER JOIN
        customer c ON p.customer_id = c.customer_id;"""
    try:
       conn = psycopg2.connect(**postgres_dbConnect)
       cur = conn.cursor()
       cur.execute(query)

        # fetch all rows
       rows = cur.fetchall()

        # column names come from cursor.description
       colnames = [desc[0] for desc in cur.description]

        # build the DataFrame
       df = pd.DataFrame(rows,columns=colnames)
       cur.close()
       conn.close()

       return df
    
    except Error as e:
        print(f"Database error: {e}")

def transformData(df:pd.DataFrame):
    try:
        #df['payment_date'] = pd.to_datetime(df['payment_date'], utc=True)
        df['full_name']=df['first_name'].str.strip()+' '+df['last_name'].str.strip()
        df=df[['payment_id','amount','payment_date','customer_id','full_name','email']]
        return df
    except Exception as e:
        print(f"Error during transformation: {e}")
        return pd.DataFrame()

def load(df):
    try:
        conn=mysql.connector.connect(**mysql_dbConnect)
        cursor=conn.cursor()
        cursor.execute("""
                        create table if not exists payment_details(
                        payment_id int primary key,
                        amount decimal(10,2),
                        payment_date date,
                        customer_id int,
                        full_name varchar(200),
                        email varchar(225)
                        )
                        """)
        insert_query="""
        INSERT INTO payment_details(payment_id,amount,payment_date,customer_id,full_name,email)
        VALUES(%s,%s,%s,%s,%s,%s)
        """
        data=[tuple(row) for row in df.values]
        cursor.executemany(insert_query,data)
        conn.commit()
    except Error as e:
        print(f"Database error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def main():
    df=getData()
    if df is not None:
        df=transformData(df)
        load(df)
        print("success")
    else:
        print("no data is saved")

if __name__ == "__main__":
    main()




   
