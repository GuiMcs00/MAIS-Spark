from pyspark.sql import functions as F, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType

spark = SparkSession.builder.appName("MigracaoMAIS").getOrCreate()

# Exemplo de DataFrame com a estrutura inicial
data = [
    ("Folder1", "2023-09-01", {"user1": [{"group": "groupA", "access_level": "read"}, {"group": "groupB", "access_level": "write"}]}),
    ("Folder2", "2023-09-02", {"user2": [{"group": "groupC", "access_level": "read"}]})
]

schema = StructType([
    StructField("Folder", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Users", MapType(StringType(), ArrayType(StructType([
        StructField("group", StringType(), True),
        StructField("access_level", StringType(), True)
    ]))))
])

df = spark.createDataFrame(data, schema)

# Explode os dicionários para registros separados
df = df.select(
    "Folder",
    "Date",
    F.explode("Users").alias("User", "GroupsAccess")  # Explode o dicionário 'Users' em 'User' e 'GroupsAccess'
)

# Explode a lista de grupos e acessos em registros separados
df = df.select(
    "Folder",
    "Date",
    "User",
    F.explode("GroupsAccess").alias("GroupAccess")  # Explode a lista dentro de cada usuário
)

# Seleciona as colunas finais com 'group' e 'access_level'
df_final = df.select(
    "Folder",
    "Date",
    "User",
    F.col("GroupAccess.group").alias("Group"),
    F.col("GroupAccess.access_level").alias("AccessLevel")
)

df_final.show(truncate=False)
