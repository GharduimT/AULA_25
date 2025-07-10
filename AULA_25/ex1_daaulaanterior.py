# código da aula anterior
from datetime import datetime
import polars as pl
import os
import gc  #  Garbage Collector

ENEDERECO_DAD0S = r'../../dados/'
ENEDERECO_DAD0S_BRONZE = r'../../bronze/'

try:
    print('obtendo os dados...')
    inicio = datetime.now()

    df_bolsa_familia = None  #  Vai iniciar vazio/ se bolsa familia estiver vazio... senão...

    #  lista dos csvs que serão processados
    lista_arquivos = []

    lista_dir_arquivos = os.listdir(ENEDERECO_DAD0S) #  fará a leitura do que tem na pasta de dados

    #  PEGANDO APENAS OS CSVS
    for arquivo in lista_dir_arquivos:
        if arquivo.endswith('.csv'):
            lista_arquivos.append(arquivo)
    # print(lista_arquivos)
    
    # leitura dos csvs
    for arquivo in lista_arquivos:
        print(f'Lendo o arquivo {arquivo}')

        df = pl.read_csv(ENEDERECO_DAD0S + arquivo, separator=';', encoding='iso8859-1')

        if df_bolsa_familia is None:
            df_bolsa_familia = df
        else:
            df_bolsa_familia= pl.concat([df_bolsa_familia, df])

        del df

        print(df_bolsa_familia.head())
        
        print(f'Arquivo {arquivo} processado com sucesso')

    print(df_bolsa_familia.head())

    # converter para float o valor da parcela
    df_bolsa_familia = df_bolsa_familia.with_columns(
        pl.col('VALOR PARCELA').str.replace(',', '.').cast(pl.Float64)
    )
    print('Dados concatenados com sucesso')
    print('Iniciando a gravação do arquivo Parquet')

    df_bolsa_familia.write_parquet(ENEDERECO_DAD0S_BRONZE + 'Bbolsa_familia.parquet') #  cria na pasta bronze um arquivo compactado dos dados coletados
    print(df_bolsa_familia.head())
    print(df_bolsa_familia.shape)

    del df_bolsa_familia

    gc.collect()

    print('Gravação do arquivo Parquet concluída com sucesso')

    fim = datetime.now()

    print(f'Tempo de execução:{fim - inicio}')

except Exception as e:
    print(f'Erro ao obter os dados')