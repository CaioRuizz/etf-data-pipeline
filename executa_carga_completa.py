import datetime

from carga_completa import download_b3_pregoes, bronze_para_silver


if __name__ == '__main__':
    hoje = datetime.datetime.today()
    download_b3_pregoes(range(1986, hoje.year + 1))
    bronze_para_silver()
