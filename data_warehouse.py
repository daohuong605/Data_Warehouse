from datetime import datetime, timedelta
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLCheckOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 1),
    "email_on_retry": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_warehouse",
    default_args=default_args,
    schedule='@daily'
) as dag:
    start_pipeline = EmptyOperator(task_id='start_pipeline')

    load_staging_dataset = EmptyOperator(task_id='load_staging_dataset')

    # Hàm để load dữ liệu từ MySQL vào Airflow
    def load_customer_dataset():
        mysql_hook = MySqlHook(mysql_conn_id='airflow_db')  
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM customer")  
        rows = cursor.fetchall()
        for row in rows:
            print(row)  
        cursor.close()
        connection.close()
    
    load_customer = PythonOperator(
        task_id='load_customer',
        python_callable=load_customer_dataset,
        dag=dag
    )

    # Load Product Line Dataset
    def load_product_line_dataset():
        mysql_hook = MySqlHook(mysql_conn_id='airflow_db')
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM Product_line ")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        cursor.close()
        connection.close()

    load_product_line = PythonOperator(
        task_id='load_product_line',
        python_callable=load_product_line_dataset,
        dag=dag
    )

    # Load Promotion Dataset
    def load_promotion_dataset():
        mysql_hook = MySqlHook(mysql_conn_id='airflow_db')
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM Promotion")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        cursor.close()
        connection.close()

    load_promotion = PythonOperator(
        task_id='load_promotion',
        python_callable=load_promotion_dataset,
        dag=dag
    )

    # Load Sale Details Dataset
    def load_sale_details_dataset():
        mysql_hook = MySqlHook(mysql_conn_id='airflow_db')
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM Sale_Detail_cs")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        cursor.close()
        connection.close()

    load_sale_details = PythonOperator(
        task_id='load_sale_details',
        python_callable=load_sale_details_dataset,
        dag=dag
    )

    # Load Sales Dataset
    def load_sales_dataset():
        mysql_hook = MySqlHook(mysql_conn_id='airflow_db')
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM Sale_New ")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        cursor.close()
        connection.close()

    load_sales = PythonOperator(
        task_id='load_sales',
        python_callable=load_sales_dataset,
        dag=dag
    )

    # Task kiểm tra dữ liệu sau khi được load vào MySQL
    def check_data_after_loading_customer(mysql_conn_id, file_path):
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        df_from_csv = pd.read_csv(file_path)
        df_from_db = pd.read_sql(f"SELECT * FROM {table_name}", con=mysql_hook.get_conn())
        if not df_from_csv.equals(df_from_db):
            raise AirflowException(f"Data from {file_path} does not match data in MySQL table {table_name}!")

    check_data_after_loading_customer_task = PythonOperator(
        task_id='check_data_after_loading_customer_task',
        python_callable=check_data_after_loading_customer,
        op_kwargs={'mysql_conn_id': 'airflow_db', 'file_path': '/home/daohuong65/airflow/dags/OLTP/customer.csv'},
        dag=dag
    )

    def check_data_after_loading_product_line(mysql_conn_id, file_path):
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        df_from_csv = pd.read_csv(file_path)
        df_from_db = pd.read_sql(f"SELECT * FROM {table_name}", con=mysql_hook.get_conn())
        if not df_from_csv.equals(df_from_db):
            raise AirflowException(f"Data from {file_path} does not match data in MySQL table {table_name}!")

    check_data_after_loading_product_line_task = PythonOperator(
        task_id='check_data_after_loading_product_line_task',
        python_callable=check_data_after_loading_product_line,
        op_kwargs={'mysql_conn_id': 'airflow_db', 'file_path': '/home/daohuong65/airflow/dags/OLTP/Product_line.csv'},
        dag=dag
    )

    def check_data_after_loading_promotion(mysql_conn_id, file_path):
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        df_from_csv = pd.read_csv(file_path)
        df_from_db = pd.read_sql(f"SELECT * FROM {table_name}", con=mysql_hook.get_conn())
        if not df_from_csv.equals(df_from_db):
            raise AirflowException(f"Data from {file_path} does not match data in MySQL table {table_name}!")

    check_data_after_loading_promotion_task = PythonOperator(
        task_id='check_data_after_loading_promotion_task',
        python_callable=check_data_after_loading_promotion,
        op_kwargs={'mysql_conn_id': 'airflow_db', 'file_path': '/home/daohuong65/airflow/dags/OLTP/Promotion.csv'},
        dag=dag
    )

    def check_data_after_loading_sale_details(mysql_conn_id, file_path):
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        df_from_csv = pd.read_csv(file_path)
        df_from_db = pd.read_sql(f"SELECT * FROM {table_name}", con=mysql_hook.get_conn())
        if not df_from_csv.equals(df_from_db):
            raise AirflowException(f"Data from {file_path} does not match data in MySQL table {table_name}!")

    check_data_after_loading_sale_details_task = PythonOperator(
        task_id='check_data_after_loading_sale_details_task',
        python_callable=check_data_after_loading_sale_details,
        op_kwargs={'mysql_conn_id': 'airflow_db', 'file_path': '/home/daohuong65/airflow/dags/OLTP/Sale_Detail_cs.csv'},
        dag=dag
    )

    def check_data_after_loading_sales(mysql_conn_id, file_path):
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        df_from_csv = pd.read_csv(file_path)
        df_from_db = pd.read_sql(f"SELECT * FROM {table_name}", con=mysql_hook.get_conn())
        if not df_from_csv.equals(df_from_db):
            raise AirflowException(f"Data from {file_path} does not match data in MySQL table {table_name}!")

    check_data_after_loading_sales_task = PythonOperator(
        task_id='check_data_after_loading_sales_task',
        python_callable=check_data_after_loading_sales,
        op_kwargs={'mysql_conn_id': 'airflow_db', 'file_path': '/home/daohuong65/airflow/dags/OLTP/Sale_New.csv'},
        dag=dag
    )


    # Create Dim Table
    create_dim_table = EmptyOperator(task_id='create_dim_table')

    # Create Dim Date Table
    create_dim_date = SQLExecuteQueryOperator(
        task_id='create_dim_date',
        conn_id='airflow_db',
        sql="""
        DROP TABLE IF EXISTS dim_date;
        CREATE TABLE dim_date AS
        SELECT 
            (@row_number:=@row_number + 1) AS Date_ID,
            DATE(Date) AS date,
            DATE_FORMAT(Date, '%W') AS day_name,
            MONTH(Date) AS month,
            DATE_FORMAT(Date, '%M') AS month_name,
            QUARTER(Date) AS quarter,
            CONCAT('Q', QUARTER(Date)) AS quarter_name,
            YEAR(Date) AS year,
            CAST(YEAR(Date) AS CHAR) AS year_name
        FROM 
            Sale_New,
            (SELECT @row_number:=0) AS dummy;
        """
    )


    # Create Dim Customer Table
    create_dim_customer = SQLExecuteQueryOperator(
        task_id = 'create_dim_customer',
        conn_id='airflow_db',
        sql="""
        DROP TABLE IF EXISTS dim_customer;
        CREATE TABLE dim_customer AS
        SELECT * FROM customer;
        """
    )

    # Create Dim Promotion Table
    create_dim_promotion = SQLExecuteQueryOperator(
        task_id='create_dim_promotion',
        conn_id='airflow_db',
        sql="""
        DROP TABLE IF EXISTS dim_promotion;
        CREATE TABLE dim_promotion AS
        SELECT * FROM Promotion;
        """
    )

    # Create Dim Product Line Table
    create_dim_product_line = SQLExecuteQueryOperator(
        task_id='create_dim_product_line',
        conn_id='airflow_db',
        sql="""
        DROP TABLE IF EXISTS dim_product_line;
        CREATE TABLE dim_product_line AS
        SELECT * FROM Product_line;
        """
    )

    # Create Fact Sales Table
    create_fact_sales = SQLExecuteQueryOperator(
    task_id='create_fact_sales',
    conn_id='airflow_db',
    sql="""
       CREATE TABLE IF NOT EXISTS Fact_Sale AS
        SELECT 
            s.Sale_ID,
            s.Customer_ID,
            sd.Product_line_id AS ProductLine_ID,
            d.Date_ID AS Date_ID,
            s.Promotion_ID, 
            sd.Quantity,
            sd.Unit_Price,
            (sd.Quantity * sd.Unit_price) AS Total_Amount,
            s.Payment
        FROM 
            Sale_New s
        JOIN 
            Sale_Detail_cs sd ON s.Sale_ID = sd.Sale_id
        JOIN 
            dim_date d ON s.Date = d.date;

        """
    )

    # Check Fact Sales Table
    check_fact_sales = SQLCheckOperator(
    task_id='check_fact_sales',
    conn_id='airflow_db',
    sql='SELECT COUNT(*) > 0 FROM Fact_Sale;'
    )


    # Finish Pipeline
    finish_pipeline = EmptyOperator(task_id='finish_pipeline')

    # Setting the task dependencies
    start_pipeline >> load_staging_dataset

    load_staging_dataset >> load_customer
    load_staging_dataset >> load_product_line
    load_staging_dataset >> load_promotion
    load_staging_dataset >> load_sale_details
    load_staging_dataset >> load_sales

    # Thêm các phụ thuộc cho các task kiểm tra dữ liệu
    load_customer >> check_data_after_loading_customer_task >> create_dim_table
    load_product_line >> check_data_after_loading_product_line_task >> create_dim_table
    load_promotion >> check_data_after_loading_promotion_task >> create_dim_table
    load_sale_details >> check_data_after_loading_sale_details_task >> create_dim_table
    load_sales >> check_data_after_loading_sales_task >> create_dim_table

    # Thêm các phụ thuộc cho các task tạo dim table và kết thúc pipeline
    create_dim_table >> create_dim_customer
    create_dim_table >> create_dim_date
    create_dim_table >> create_dim_promotion
    create_dim_table >> create_dim_product_line
    [create_dim_customer, create_dim_date, create_dim_promotion, create_dim_product_line] >> create_fact_sales
    create_fact_sales >> check_fact_sales
    check_fact_sales  >> finish_pipeline
