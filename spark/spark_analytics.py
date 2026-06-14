import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

HDFS_RAW       = "hdfs:///user/projeto/bolsafamilia/raw/"
HDFS_PROCESSED = "hdfs:///user/projeto/bolsafamilia/processed/"
ESTADOS_Q5     = ["SP", "MG", "RJ"]

SCHEMA = StructType([
    StructField("MES_COMPETENCIA",        StringType(), True),
    StructField("MES_REFERENCIA",         StringType(), True),
    StructField("UF",                     StringType(), True),
    StructField("CODIGO_MUNICIPIO_SIAFI", StringType(), True),
    StructField("NOME_MUNICIPIO",         StringType(), True),
    StructField("CPF_FAVORECIDO",         StringType(), True),
    StructField("NIS_FAVORECIDO",         StringType(), True),
    StructField("NOME_FAVORECIDO",        StringType(), True),
    StructField("VALOR_PARCELA",          StringType(), True),
])

spark = (
    SparkSession.builder
    .appName("BolsaFamilia_2021_PySpark")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 65)
print("  PROJETO INTEGRADOR — PySpark  |  Bolsa Família 2021 (Jan-Mar)")
print("=" * 65 + "\n")

print(">>> [PASSO 1] Lendo CSVs e convertendo para Parquet (Snappy)...")
t0 = time.time()

df_raw = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .option("encoding", "ISO-8859-1")
    .option("quote", '"')
    .schema(SCHEMA)
    .csv(HDFS_RAW)
)

df_tipado = (
    df_raw
    .withColumn("UF",
        F.trim(F.upper(F.regexp_replace(F.col("UF"), '"', ''))))
    .withColumn("NOME_MUNICIPIO",
        F.trim(F.upper(F.regexp_replace(F.col("NOME_MUNICIPIO"), '"', ''))))
    .withColumn("VALOR_PARCELA",
        F.regexp_replace(
            F.regexp_replace(F.col("VALOR_PARCELA"), '\\.', ''),
            ',', '.'
        ).cast(DoubleType()))
    .filter(F.col("UF").isNotNull() & (F.col("UF") != ""))
    .filter(F.col("NOME_MUNICIPIO").isNotNull() & (F.col("NOME_MUNICIPIO") != ""))
    .filter(F.col("VALOR_PARCELA").isNotNull())
)

df_tipado.write.mode("overwrite").parquet(HDFS_PROCESSED)
t1 = time.time()

total = df_tipado.count()
print(f"    Registros processados : {total:,}")
print(f"    Parquet salvo em      : {HDFS_PROCESSED}")
print(f"    Tempo de conversão    : {t1 - t0:.2f}s\n")

print(">>> [PASSO 2] Carregando Parquet para as 5 consultas...\n")
df = spark.read.parquet(HDFS_PROCESSED).cache()
SEP = "─" * 65

# Q1
print(SEP)
print("Q1 — Beneficiados por UF")
print(SEP)
t_q = time.time()
(df.groupBy("UF")
   .agg(F.count("*").alias("QTD_BENEFICIADOS"))
   .orderBy(F.col("QTD_BENEFICIADOS").desc())
   .show(30, truncate=False))
print(f"    Tempo Q1: {time.time()-t_q:.2f}s\n")

# Q2
print(SEP)
print("Q2 — Top 50 cidades com mais beneficiados (Brasil)")
print(SEP)
t_q = time.time()
(df.groupBy("UF", "NOME_MUNICIPIO")
   .agg(F.count("*").alias("QTD_BENEFICIADOS"))
   .orderBy(F.col("QTD_BENEFICIADOS").desc())
   .limit(50)
   .show(50, truncate=False))
print(f"    Tempo Q2: {time.time()-t_q:.2f}s\n")

# Q3
print(SEP)
print("Q3 — Total de pagamentos por UF (R$)")
print(SEP)
t_q = time.time()
(df.groupBy("UF")
   .agg(F.round(F.sum("VALOR_PARCELA"), 2).alias("TOTAL_PAGO_RS"))
   .orderBy(F.col("TOTAL_PAGO_RS").desc())
   .show(30, truncate=False))
print(f"    Tempo Q3: {time.time()-t_q:.2f}s\n")

# Q4
print(SEP)
print("Q4 — Top 10 cidades em SP e BA por valor total")
print(SEP)
t_q = time.time()
(df.filter(F.col("UF").isin("SP", "BA"))
   .groupBy("UF", "NOME_MUNICIPIO")
   .agg(F.round(F.sum("VALOR_PARCELA"), 2).alias("TOTAL_PAGO_RS"))
   .orderBy(F.col("TOTAL_PAGO_RS").desc())
   .limit(10)
   .show(10, truncate=False))
print(f"    Tempo Q4: {time.time()-t_q:.2f}s\n")

# Q5
print(SEP)
print(f"Q5 — Top 100 cidades de {ESTADOS_Q5} com mais beneficiados")
print(SEP)
t_q = time.time()
(df.filter(F.col("UF").isin(ESTADOS_Q5))
   .groupBy("UF", "NOME_MUNICIPIO")
   .agg(F.count("*").alias("QTD_BENEFICIADOS"))
   .orderBy(F.col("QTD_BENEFICIADOS").desc())
   .limit(100)
   .show(100, truncate=False))
print(f"    Tempo Q5: {time.time()-t_q:.2f}s\n")

print("=" * 65)
print("  EXECUÇÃO CONCLUÍDA")
print(f"  Total de registros no trimestre : {total:,}")
print(f"  Tempo de conversão CSV→Parquet  : {t1 - t0:.2f}s")
print("=" * 65)

spark.stop()
