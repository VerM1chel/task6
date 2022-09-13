from snowflake_hook import SnowflakeHook
from airflow.models.baseoperator import BaseOperator

class SnowflakeCreater():
    def __init__(self, account, database, warehouse, conn_id, **kwargs) -> None:
        super().__init__(**kwargs)
        self.account = account
        self.database = database
        self.warehouse = warehouse
        self.conn_id = conn_id
        self.hook = SnowflakeHook(conn_id=self.conn_id, account=self.account, database=self.database,
                                  warehouse=self.warehouse)
        self.connect = self.hook.get_conn()

    def execute(self):
        super().execute('''create table IF NOT EXISTS raw_table 
            (
                _id text,
                IOS_App_Id text,
                Title text,
                Developer_Name text,
                Developer_IOS_Id text,
                IOS_Store_Url text,
                Seller_Official_Website text,
                Age_Rating text,
                Total_Average_Rating text,
                Total_Number_of_Ratings text,
                Average_Rating_For_Version text,
                Number_of_Ratings_For_Version text,
                Original_Release_Date text,
                Current_Version_Release_Date text,
                Price_USD text,
                Primary_Genre text,
                All_Genres text,
                Languages text,
                Description text
            )''')

        super().execute('''create table if not exists stage_table
            (
                _id varchar(24),
                IOS_App_Id varchar(24),
                Title text,
                Developer_Name text,
                Developer_IOS_Id varchar(24),
                IOS_Store_Url text,
                Seller_Official_Website text,
                Age_Rating varchar(25),
                Total_Average_Rating float,
                Total_Number_of_Ratings float,
                Average_Rating_For_Version float,
                Number_of_Ratings_For_Version float,
                Original_Release_Date timestamp_tz,
                Current_Version_Release_Date timestamp_tz,
                Price_USD float,
                Primary_Genre varchar(50),
                All_Genres array,
                Languages array,
                Description text
            )''')

        super().execute('''create stage if not exists stage_storage
                                file_format = (type = 'CSV' field_delimiter = ',' skip_header = 1)''')

        super().execute('''create stream if not exists raw_stream on table raw_table''')
        super().execute('''create or replace stream stage_stream on table stage_table''')
