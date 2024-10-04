from pyspark.sql import SparkSession

from extract_data import read_task, organize_task

def main():

    spark = SparkSession.builder.appName("MigracaoMAIS").getOrCreate()

    # Task 1: Extracting data
    df_103, df_207 = read_task(spark)

    # Task 2: Organizing data
    organized_103 = organize_task(df_103)
    organized_207 = organize_task(df_207)

    organized_103.show(truncate=False)
    organized_207.show(truncate=False)


if __name__ == "__main__":
    main()
