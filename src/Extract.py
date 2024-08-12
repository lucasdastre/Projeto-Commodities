#Import

#elt example

import pandas as pd
from  sqlalchemy import create_engine
from  dotenv import load_dotenv
import yfinance as yf
import os

load_dotenv()

#import variaveis de ambiente

commodities = ['CL=F' , 'GC=F' , 'SI=F']


#Pra nao ficar em hard code devemos criar as variaveis de ambiente seprado do código
DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA =  os.getenv('DB_SCHEMA_PROD')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

def buscar_dados_commodities(simbolo , periodo = '5d' , intervalo = '1d'):
    ticker = yf.Ticker(simbolo)
    dados = ticker.history(period = periodo , interval = intervalo)[['Close']]
    dados['simbolo'] = simbolo
    return dados  #retorna um dataframe

def concat_dados(commodities):
    todos_dados = []
    for simbolo in commodities:
        dados = buscar_dados_commodities(simbolo)
        todos_dados.append(dados)
    return pd.concat(todos_dados)


def salvar_postgres(df , schema = 'public'):
    df.to_sql('commodities' , engine , if_exists = 'replace' , index = True , index_label = 'Date' , schema = schema) #proprio pandas está criando o schema



if __name__ == "__main__":
    dados_concatenados = concat_dados(commodities)
    salvar_postgres(dados_concatenados)
