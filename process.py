from db import DataBase

from pyspark.sql import SparkSession


class Process:
    def __init__(self, source_file_path):
        self.build_spark_session()
        self.source_file_path = source_file_path

    def build_spark_session(self):
        self.spark = SparkSession.builder.appName("Postman").getOrCreate()

    def get_source_file(self):
        return self.source_file_path

    def read_file(self):
        source_file = self.get_source_file()
        return self.spark.read.csv(
            source_file, header=True, quote='"', escape='"', multiLine=True
        )

    def get_source_data(self):
        return self.read_file()

    def transform(self, df):
        return df

    def load(self, df):
        TABLE_NAME = 'PRODUCTS'
        pandas_df = df.toPandas()
        db = DataBase()
        conn = db.get_connection()
        pandas_df.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
        db.show(TABLE_NAME)
        conn.close()
        return df

    def run(self):
        df = self.get_source_data()
        df = self.transform(df)
        self.load(df)
        return df
