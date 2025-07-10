from datetime import datetime
import pandas as pd
import polars as pl
import numpy as np
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


#  processasndo as informações
try:
    array_valor_parcela = np.array(df_bolsa_familia['VALOR PARCELA'])
    media = np.mean(array_valor_parcela)
    mediana = np.median(array_valor_parcela)
    distancia_media_mediana = abs(media - mediana) / mediana

    print(f'MEDIA: {media}')
    print(f'MEDIANA: {mediana}')
    print(f'DISTANCIA MEDIA E MEDIANA: {distancia_media_mediana}')




except Exception as e:
    print(f'Erro ao Processar as informações')

# Tempo de Execução: 0:00:12.814181
# Arquivo Parquet lido com sucesso
# MEDIA: 667.5678859340816
# MEDIANA: 650.0
# DISTANCIA MEDIA E MEDIANA: 0.02702751682166396