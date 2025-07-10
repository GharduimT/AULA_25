# como nessa etapa A PROPOSTA É LER UM ARQUIVO PARQUET JÁ GRAVADO FOI NECESSÁRIO INSTALLAR A BIBLIOTECA FASTPARQUET
#  Tempo de Execução: 0:00:31.419212
# from datetime import datetime
# import pandas as pd
# import polars as pl

# ENDERECO_DADOS = '../../bronze/'

# try:
#     print ('Iniciando a leitura do arquivo Parquet')
#     inicio = datetime.now()

#     df_bolsa_familia = pd.read_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')
#     print(df_bolsa_familia.head())
    
#     final = datetime.now()
#     print(f'Tempo de Execução: {final - inicio}')
#     print('Arquivo Parquet lido com sucesso')


# except Exception as e:
#     print(f'Erro ao obter dados {e}')


#  Tempo de Execução: 0:00:11.978777
from datetime import datetime
import pandas as pd
import polars as pl

ENDERECO_DADOS = '../../bronze/'

try:
    print ('Iniciando a leitura do arquivo Parquet')
    inicio = datetime.now()

    df_bolsa_familia = pl.read_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')
    print(df_bolsa_familia.head())
    
    final = datetime.now()
    print(f'Tempo de Execução: {final - inicio}')
    print('Arquivo Parquet lido com sucesso')


except Exception as e:
    print(f'Erro ao obter dados {e}')

