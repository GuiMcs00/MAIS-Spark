from pyspark.sql.functions import monotonically_increasing_id

def informacao_map(df):
    df_informacao = df.select('Folder').withColumn('temp_id', monotonically_increasing_id())

    df_informacao.head()
    df_informacao.tail(num=10)

    return df_informacao


