import requests
import zipfile
import io
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, substring, to_date, split


def encontra_arquivo_mais_recente(diretorio):
    arquivos = [f for f in os.listdir(diretorio) if os.path.isfile(os.path.join(diretorio, f))]

    if not arquivos:
        print("O diretório está vazio.")
        return None

    # ObtÃ©m a data de modificaÃ§Ã£o de cada arquivo
    datas_modificacao = [os.path.getmtime(os.path.join(diretorio, f)) for f in arquivos]

    # Encontra o Ã­ndice do arquivo mais recente
    indice_mais_recente = datas_modificacao.index(max(datas_modificacao))

    # ObtÃ©m o nome do arquivo mais recente
    arquivo_mais_recente = arquivos[indice_mais_recente]

    # Retorna o caminho completo do arquivo mais recente
    return os.path.join(diretorio, arquivo_mais_recente)


base_path = os.path.abspath(os.path.dirname(__file__))
base_url = 'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A{ano}.ZIP'


def download_b3_pregoes(anos: range):
    spark = SparkSession.builder.getOrCreate()

    for ano in anos:
        print(f'ETL ano {ano}')

        print('Baixando arquivo')
        zip_file_url = base_url.format(ano=ano)
        r = requests.get(zip_file_url)

        print('Extraindo arquivo')
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(base_path)

        print('Formatando arquivo')

        arquivo_cru = encontra_arquivo_mais_recente(base_path)

        df = spark.read.text(arquivo_cru)

        df = df.select(
            df.value.substr(3, 8).alias("Data"),
            df.value.substr(11, 2).alias("CodBdi"),
            df.value.substr(13, 12).alias("CodNeg"),
            df.value.substr(25, 3).alias("TipoMercado"),
            df.value.substr(28, 12).alias("Empresa"),
            df.value.substr(40, 10).alias("Especificacao"),
            df.value.substr(50, 3).alias("Prazo"),
            df.value.substr(53, 4).alias("Moeda"),
            df.value.substr(57, 13).alias("PrecoAbertura"),
            df.value.substr(70, 13).alias("PrecoMaximo"),
            df.value.substr(83, 13).alias("PrecoMinimo"),
            df.value.substr(96, 13).alias("PrecoMedio"),
            df.value.substr(109, 13).alias("PrecoUltimo"),
            df.value.substr(122, 13).alias("MelhorOfertaCompra"),
            df.value.substr(135, 13).alias("MelhorOfertaVenda"),
            df.value.substr(148, 5).alias("NumeroNegocio"),
            df.value.substr(153, 18).alias("QuantidadeNegociada"),
            df.value.substr(171, 18).alias("VolumeFinanceiro"),
            df.value.substr(189, 12).alias("PrecoExercicio"),
            df.value.substr(202, 1).alias("IndicadorCorrecao"),
            df.value.substr(203, 8).alias("DataVencimento"),
            df.value.substr(211, 7).alias("FatorCotacao"),
            df.value.substr(218, 13).alias("PrecoExercicioPontos"),
            df.value.substr(231, 12).alias("CodIsin"),
            df.value.substr(243, 3).alias("NumeroDistribuicao"),
        ).where(trim(col("QuantidadeNegociada")) != '')

        df = df.withColumn("Data", to_date(substring(trim(col("Data")), 1, 8), "yyyyMMdd"))
        df = df.withColumn("CodBdi", trim(col("CodBdi").cast("integer")))
        df = df.withColumn("CodNeg", trim(col("CodNeg")))
        df = df.withColumn("TipoMercado", trim(col("TipoMercado").cast("integer")))
        df = df.withColumn("Empresa", trim(col("Empresa")))
        df = df.withColumn("Especificacao", split(col("Especificacao"), " ")[0])
        df = df.withColumn("Prazo", trim(col("Prazo")).cast("integer"))
        df = df.withColumn("Moeda", trim(col("Moeda")))
        df = df.withColumn("PrecoAbertura", trim(col("PrecoAbertura")).cast("Decimal(18, 2)") / 100)
        df = df.withColumn("PrecoMaximo", trim(col("PrecoMaximo")).cast("Decimal(18, 2)") / 100)
        df = df.withColumn("PrecoMinimo", trim(col("PrecoMinimo")).cast("Decimal(18, 2)") / 100)
        df = df.withColumn("PrecoMedio", trim(col("PrecoMedio")).cast("Decimal(18, 2)") / 100)
        df = df.withColumn("PrecoUltimo", trim(col("PrecoUltimo")).cast("Decimal(18, 2)") / 100)
        df = df.withColumn("MelhorOfertaCompra", trim(col("MelhorOfertaCompra")).cast("Decimal(18, 2)") / 100)
        df = df.withColumn("MelhorOfertaVenda", trim(col("MelhorOfertaVenda")).cast("Decimal(18, 2)") / 100)
        df = df.withColumn("NumeroNegocio", trim(col("NumeroNegocio")).cast("Decimal(18, 2)"))
        df = df.withColumn("QuantidadeNegociada", trim(col("QuantidadeNegociada")).cast("Decimal(18, 2)"))
        df = df.withColumn("VolumeFinanceiro", trim(col("VolumeFinanceiro")).cast("Decimal(18, 2)"))
        df = df.withColumn("PrecoExercicio", trim(col("PrecoExercicio")).cast("Decimal(18, 2)"))
        df = df.withColumn("IndicadorCorrecao", trim(col("IndicadorCorrecao")).cast("integer"))
        df = df.withColumn("DataVencimento", to_date(substring(trim(col("DataVencimento")), 1, 8), "yyyyMMdd"))
        df = df.withColumn("FatorCotacao", trim(col("FatorCotacao")).cast("bigint"))
        df = df.withColumn("PrecoExercicioPontos", trim(col("PrecoExercicioPontos")).cast("bigint"))
        df = df.withColumn("CodIsin", trim(col("CodIsin")))
        df = df.withColumn("NumeroDistribuicao", trim(col("NumeroDistribuicao")).cast("bigint"))

        print('Gravando arquivo formatado')

        print(f'Ano {ano}: {df.count()} registros')

        df.write.mode('overwrite').parquet(f'./files/bronze/negociacao/Ano={ano}')

        print('Excluindo arquivo cru')

        os.remove(arquivo_cru)
