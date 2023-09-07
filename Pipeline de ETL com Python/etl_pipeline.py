import pandas as pd

print("Iniciando o Pipeline de ETL")

# Passo 1: Extração
def extrair_dados(arquivo_csv):
    df = pd.read_csv(arquivo_csv)
    return df


# Passo 2: Transformação
def transformar_dados(df):
    # Exemplo de transformação: duplicar a coluna 'valor'
    df['valor_duplicado'] = df['valor'] * 2
    return df


# Passo 3: Carregamento
def carregar_dados(df, arquivo_saida):
    df.to_csv(arquivo_saida, index=False)
    print(f"Dados carregados em '{arquivo_saida}' com sucesso!")


# Nome dos arquivos de entrada e saída
arquivo_entrada = 'dados_originais.csv'
arquivo_saida = 'dados_transformados.csv'

# Executar o Pipeline ETL
dados_extraidos = extrair_dados(arquivo_entrada)
dados_transformados = transformar_dados(dados_extraidos)
carregar_dados(dados_transformados, arquivo_saida)

print("Pipeline de ETL concluído com sucesso!")
