import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession

from src.handlers import informacao_operation


@pytest.fixture(scope="session")
def spark():
    conf = SparkConf() \
        .setAppName("Teste") \
        .setMaster("local[*]") \
        .set("spark.driver.extraClassPath", "C:/jdbc/jars/*")

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    yield spark


def test_insert_informacao_successfully(spark):

    data = [(r'\\sbcdf103\arquivos\SECRE\Sucon', 0), (r'\\sbcdf103\secre$\Sucon\Colegiados', 1)]
    columns = ['Folder', 'temp_id']
    df_parameter = spark.createDataFrame(data, columns)

    existing_records, novos_ids = informacao_operation(spark, df_parameter)

    # Testando se os dados foram processados corretamente
    assert existing_records.count() >= 0, "Deve haver registros existentes ou nenhum"
    assert novos_ids is not None, "Deve haver novos IDs retornados"
    assert novos_ids.count() > 0, "Deve haver registros inseridos no banco de dados"

    # Caso queira verificar se os dados estão corretos, pode fazer mais asserts específicos:
    expected_new_records = [r'\\sbcdf103\arquivos\SECRE\Sucon', r'\\sbcdf103\secre$\Sucon\Colegiados']
    actual_new_records = [row.DESCRICAO for row in novos_ids.collect()]
    assert set(expected_new_records) == set(actual_new_records), "Os registros novos devem estar no banco"