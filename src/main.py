from pyspark.sql import SparkSession

from reading import reading_task

def main():

    spark = SparkSession.builder.appName("MigracaoMAIS").getOrCreate()

    reading_task(spark)


if __name__ == "__main__":
    main()
