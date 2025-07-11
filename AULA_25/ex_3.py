from datetime import datetime
import pandas as pd
import polars as pl
import numpy as np
import matplotlib.pyplot as plt

ENDERECO_DADOS = '../../bronze/'

try:
    print('Iniciando a leitura do arquivo Parquet')
    inicio = datetime.now()
        
    df_bolsa_familia = (
        pl.scan_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')
        .select(pl.col(['VALOR PARCELA', 'NOME MUNICÍPIO']))
        .filter(pl.col('VALOR PARCELA') > 3990)
    )

    df_bolsa_familia = df_bolsa_familia.collect()

    print(df_bolsa_familia.head())
    
    final = datetime.now()
    print(f'Tempo de Execução: {final - inicio}')
    print('Arquivo Parquet lido com sucesso')


except Exception as e:
    print(f'Erro ao obter dados {e}')


#  processasndo...
try:
    array_valor_parcela = np.array(df_bolsa_familia['VALOR PARCELA'])
    
except Exception as e:
    print(f'Erro ao Processar as informações{e}')

# Visualiando a distribuição
    
try:
    print('Visualizando a Distribuição')

    #  criar boxplot
    plt.boxplot(array_valor_parcela, showmeans=True, vert=False)
    plt.title('Distribuição das Parcelas')
    plt.show()

except Exception as e:
    print(f'Erro ao plotar o painel {e}')