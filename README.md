# Migração de dados com PySpark

##  Descrição:

Este projeto realiza uma migração de dados usando PySpark, seguindo o fluxo de um processo ETL
(Extração, Transformação e Carga). Ele extrai informações de grandes arquivos .txt, organiza os dados em
estruturas compatíveis com tabelas de banco de dados e insere os registros de forma eficiente. O script 
também implementa a lógica necessária para capturar IDs de registros recém-inseridos e utilizá-los como
chaves estrangeiras em outras tabelas, garantindo a integridade referencial no banco de dados.

## Uso do PySpark

Dado o grande volume de dados processados e as frequentes interações com o banco de dados, o PySpark
é utilizado para otimizar o desempenho. O script aproveita o processamento distribuído dos RDDs (Resilient
Distributed Datasets) para particionar as operações e organizar os dados em DataFrames. Isso permite uma
manipulação e inserção eficaz dos dados nas tabelas do banco de dados.


---

## Configuração do Ambiente Spark

### 1. Instalação do JDK 11

Caso o JDK 11 ainda não esteja instalado, é necessário baixá-lo e configurá-lo adequadamente. O JDK pode ser obtido no [site oficial da Oracle](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html).

### 2. Configurar a variável de ambiente `JAVA_HOME`

Configure a variável de ambiente `JAVA_HOME` para apontar para o diretório de instalação do JDK 11.  
Exemplo de caminho para Windows:
```bash
C:\Program Files\Java\jdk-11
```

### 3. Configurar a variável de ambiente `PYSPARK_PYTHON`

Defina a variável de ambiente `PYSPARK_PYTHON` para garantir que o PySpark utilize a versão correta do Python.  
Exemplo de caminho para o Python em um ambiente virtual:
```bash
C:\Users\seu_usuário\envs\projeto\Scripts\python.exe
```

---

## Uso com SQL Server

### 1. Baixar o driver JDBC para SQL Server

Faça o download do driver JDBC necessário para conectar o PySpark ao SQL Server. O driver pode ser obtido em [Microsoft JDBC Driver for SQL Server](https://docs.microsoft.com/pt-br/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server).

### 2. Configurar o caminho do `.jar` no SparkConf

Adicione o caminho do arquivo `.jar` do JDBC nas configurações do Spark para garantir que o driver seja carregado corretamente durante a execução do script.

Exemplo de configuração no PySpark:
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.driver.extraClassPath", "/caminho/para/mssql-jdbc-9.4.0.jre11.jar") \
    .getOrCreate()
```

### 3. Autenticação Windows ou Autenticação Confiável

Se estiver utilizando autenticação integrada ou autenticação do Windows no SQL Server, será necessário adicionar o arquivo `.dll` no diretório `bin` do JDK. Certifique-se de que a arquitetura do `.dll` corresponde à do JDK (x86 ou x64).

Exemplo de caminho para o `.dll`:
```bash
C:\jdbc\auth\x86\mssql-jdbc_auth-12.8.1.x86.dll
```

---

## Uso com PostgreSQL

### 1. Baixar o driver JDBC para PostgreSQL

O driver JDBC para PostgreSQL pode ser baixado no [site oficial](https://jdbc.postgresql.org/). Certifique-se de baixar a versão mais recente para garantir compatibilidade.

### 2. Configurar o caminho do `.jar` no SparkConf

Adicione o caminho do arquivo `.jar` do driver JDBC do PostgreSQL nas configurações do Spark.

Exemplo de configuração no PySpark:
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.driver.extraClassPath", "/caminho/para/postgresql-42.2.19.jar") \
    .getOrCreate()
```

---

## Cuidados com Compatibilidades

É importante garantir a compatibilidade entre as versões do Spark, PySpark, Python, Java e o driver JDBC. Verifique a [tabela de compatibilidade](https://community.cloudera.com/t5/Community-Articles/Spark-Python-Supportability-Matrix/ta-p/379144) para assegurar que todas as versões sejam compatíveis.

No seu projeto:
- **Python:** 3.10 (já configurado no ambiente virtual Poetry)
- **PySpark:** 3.5.2 (já configurado no ambiente virtual Poetry)

---

Essa separação facilita o uso dependendo do banco de dados que você está utilizando (SQL Server ou PostgreSQL).