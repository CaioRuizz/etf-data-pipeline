from .download import download_b3_pregoes
from .filtro_etf import bronze_para_silver
from .seleciona_colunas_etfs_precos import seleciona_colunas_etf_precos

download_b3_pregoes(range(1986, 2025))
bronze_para_silver()
seleciona_colunas_etf_precos()
