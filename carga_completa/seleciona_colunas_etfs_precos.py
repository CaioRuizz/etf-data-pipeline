from pyspark.sql import SparkSession

def seleciona_colunas_etf_precos():
    spark = SparkSession.builder.getOrCreate()
    negocicacao_etf_silver = spark.read.parquet('./files/silver/negociacao_etf')
    negociacao_etf_gold = negocicacao_etf_silver.select(['CodNeg', 'Data', 'PrecoAbertura', 'PrecoUltimo'])
    negociacao_etf_gold.write.mode('overwrite').partitionBy('CodNeg').parquet('./files/gold/precos_etf')
