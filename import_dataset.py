from sqlalchemy import create_engine
import pandas as pd

# Kết nối đến cơ sở dữ liệu MySQL
engine = create_engine('mysql://airflow_user:airflow_pass@127.0.0.1:3306/final_datawarehouse')

# Đọc từng dataset từ các tệp cục bộ
file_paths = ['/home/daohuong65/airflow/dags/OLTP/customer.csv',
              '/home/daohuong65/airflow/dags/OLTP/Product_line.csv',
              '/home/daohuong65/airflow/dags/OLTP/Promotion.csv',
              '/home/daohuong65/airflow/dags/OLTP/Sale_Detail_cs.csv',
              '/home/daohuong65/airflow/dags/OLTP/Sale_New.csv']

for file_path in file_paths:
    # Đọc dữ liệu từ tệp CSV
    data = pd.read_csv(file_path)
    
    # Lấy tên bảng từ tên tệp
    table_name = file_path.split('/')[-1].split('.')[0]
    
    # Ghi dữ liệu vào MySQL
    data.to_sql(table_name, con=engine, if_exists='replace', index=False)
