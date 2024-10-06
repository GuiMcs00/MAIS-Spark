


server = "LOGAN"
src_db = "MAIS"
src_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

# URL de conexão JDBC com o SQL Server
src_url = f"jdbc:sqlserver://{server}:1433;databaseName={src_db};integratedSecurity=true;trustServerCertificate=true;encrypt=false"


def health_check(spark):

    # Definir a query SQL ou a tabela que deseja buscar
    sql = "SELECT * FROM NIVEL_ACESSO"  # Substitua pelo nome da tabela ou query SQL desejada

    # Lendo os dados do banco de dados com JDBC
    dfs = spark.read \
        .format("jdbc") \
        .option("driver", src_driver) \
        .option("url", src_url) \
        .option("dbtable", f"({sql}) AS t")\
        .load()
    # Exibindo os dados lidos
    dfs.show()


def informacao_operation(spark, df_informacao):
    def charge_database_to_df():
        query = 'SELECT * FROM INFORMACAO'

        informacao_database_df = spark.read \
            .format("jdbc") \
            .option("driver", src_driver) \
            .option("url", src_url) \
            .option("dbtable", "INFORMACAO") \
            .load()

        return informacao_database_df

    informacao_database_df = charge_database_to_df()
    df_with_ids = df_informacao.join(informacao_database_df, df_informacao.Folder == informacao_database_df.DESCRICAO, how="left")

    existing_records = df_with_ids.filter(df_with_ids["id"].isNotNull())
    new_records = df_with_ids.filter(df_with_ids["id"].isNull())

    if new_records.count() > 0:
        new_records.select("Folder") \
            .withColumnRenamed("Folder", "DESCRICAO") \
            .write \
            .format("jdbc") \
            .option("driver", src_driver) \
            .option("url", src_url) \
            .option("dbtable", "INFORMACAO") \
            .mode("append") \
            .save()

        # Após a inserção, recarregar os novos IDs do banco de dados
        novos_ids = charge_database_to_df().join(
            new_records.select("Folder").withColumnRenamed("Folder", "DESCRICAO"),
            on="DESCRICAO",
            how="inner"
        )
    else:
        novos_ids = None

    return existing_records, novos_ids
