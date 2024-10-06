from pyspark import SparkConf
from pyspark.sql import SparkSession

from src.handlers.extract_data import read_task, organize_task
from src.handlers.dataframe_manipulations import informacao_map


def main():

    conf = SparkConf() \
        .setAppName("MigracaoMAIS") \
        .setMaster("local[*]") \
        .set("spark.driver.extraClassPath", "C:/jdbc/jars/*")

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()


    # Task 1: Extracting data
    df_103, df_207 = read_task(spark)

    # Task 2: Organizing data
    organized_103 = organize_task(df_103)
    organized_207 = organize_task(df_207)

    main_df = organized_103.union(organized_207)

    main_df.show(truncate=False)

    df_informacao = informacao_map(main_df)
    df_informacao.show(truncate=False)


if __name__ == "__main__":
    main()
