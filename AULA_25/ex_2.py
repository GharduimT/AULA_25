from datetime import datetime
import pandas as pd
import polars as pl
#utilizando o pl.scan_parquet vai printar um plano/rota de execução
ENDERECO_DADOS = '../../bronze/'

try:
    print ('Iniciando a leitura do arquivo Parquet')
    inicio = datetime.now()
    
    
    df_bolsa_familia = pl.scan_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')

    df_bolsa_familia = df_bolsa_familia.collect()

    print(df_bolsa_familia.head())

    
    final = datetime.now()
    print(f'Tempo de Execução: {final - inicio}')
    print('Arquivo Parquet lido com sucesso')


except Exception as e:
    print(f'Erro ao obter dados {e}')
