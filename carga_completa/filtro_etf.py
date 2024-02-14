import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def bronze_para_silver():
    spark = SparkSession.builder.getOrCreate()

    etfs = requests.get('https://sgk6cc5a25.execute-api.sa-east-1.amazonaws.com/Prod/api/v1/etf/tickers').json()
    etfs = [i['ticker'] for i in etfs]

    negociacao = spark.read.parquet('./files/bronze/negociacao')

    negocicacao_silver = negociacao.where(col('CodNeg').isin(etfs))\
        .select(['Data',
                 'CodNeg',
                 'CodBdi',
                 'Empresa',
                 'Especificacao',
                 'Moeda',
                 'PrecoAbertura',
                 'PrecoUltimo',
                 'NumeroNegocio',
                 'VolumeFinanceiro',
                 'QuantidadeNegociada',
                 ])

    negocicacao_silver.write.mode('overwrite').partitionBy('CodNeg').parquet('./files/silver/negociacao_etf')
