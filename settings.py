# Parametros do banco de dados
DB_KEY = 'FortbrasRPA'
DB_ENC = '5QxJNLWe8Aqzn0DFY5npxbNMsVEAnl6ES+Utu6qdDM4='
DB_USER = 'alvarez'
DB_SERVER = '10.200.47.189'
# DB_NAME = 'ORQUESTRADOR'
DB_NAME = 'TESTE'
CNN_STRING = f"Driver={{SQL Server}};Server={DB_SERVER};Database={DB_NAME};Uid={DB_USER};Pwd="

# Parametros do Fluig
AMBIENTE_FLUIG = 'PRODUCAO'

# Configs Gerais
IDBOT = 46
DIAS_VISTORIA_CORTE = 100
DIAS_LIMITE_VISTORIA = 20

# Configs colunas dos relatorios
COLUNAS_MERGE_SUFRAMA = ['cnpjRemetenteFmt', 'numeroNf']
COLUNAS_MERGE_FLUIG = ['CNPJFORNECEDOR', 'NUMERONOTAFISCAL']
COLUNAS_FLUIG_UTILIZADAS = ['STATUS', 'NUM_PROCES', 'CHAVEACESSONFCOMPRA']
COLUNAS_UTILIZADAS_NO_PROCESSO = ['cnpjRemetenteFmt', 'numeroNf', 'STATUS', 'NUM_PROCES', 'qtdDias', 'CHAVEACESSONFCOMPRA', 'razaoRemetente', 'dataEmissaoNfeFmt']