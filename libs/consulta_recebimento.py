import os
import json
import requests
import pandas as pd
from settings import *
from datetime import datetime
from typing import TYPE_CHECKING
from utils_aem import utils, utils_web
from libs.exceptions import log_exceptions
from sistemas.suframa import suframa_selenium
from sistemas.fluig import fluig_request
if TYPE_CHECKING:
    from pandas import DataFrame

class ConsultaRecebimento():
    
    
    def __init__(self):
        self.url_recebimento =  'https://simnac.suframa.gov.br/#/confirmar-recebimento-mercadoria'
        self.suframa = suframa_selenium.SuframaSelenium()
        self.utils_web = utils_web.UtilsWeb()
        self.utils = utils.Utils()

    @log_exceptions
    def pegar_solicitacoes_fluig(self, login:str, senha:str, data_inicial:str, data_final:str) -> list[dict]:
        payload = {
            "name": "ds_sql_lnfr_relatorio_solicitacoes",
            "constraints": [
                {
                    "_field": "START_DATE",
                    "_initialValue": data_inicial,
                    "_finalValue": data_final,
                    "_type": 1,
                    "_likeSearch": True
                },
                {
                    "_field": "STATUS",
                    "_initialValue": 0,
                    "_finalValue": 0,
                    "_type": 2,
                    "_likeSearch": True
                },
                {
                    "_field": "STATUS",
                    "_initialValue": 1,
                    "_finalValue": 1,
                    "_type": 2,
                    "_likeSearch": True
                },
                {
                    "_field": "STATUS",
                    "_initialValue": 2,
                    "_finalValue": 2,
                    "_type": 2,
                    "_likeSearch": True
                }
                        ],
                        "fields": None,
                        "order": []
            }
        
        fluig = fluig_request.FluigRequest(login, senha, 'PRODUCAO')
        fluig.logar_fluig()
        response = fluig.session.post('https://fluig.fortbras.com.br:8444/api/public/ecm/dataset/datasets', json=payload)
        if response.status_code != 200:
            raise Exception(f'Consulta relatorio solicitacoes status code {response.status_code}')
        return json.loads(response.text).get('content').get('values')

    @log_exceptions
    def pegar_itens_suframa(self, usuario, senha) -> list[dict]:
        self.suframa.logar_suframa(usuario, senha)
        url = "https://appsimnac.suframa.gov.br/ConfirmarRecebimentoMercadoriaGrid"

        querystring = {"servico":"ConfirmarRecebimentoMercadoriaGrid","columns.0":"UF","columns.1":"CNPJ Remetente","columns.2":"Razão Social","columns.3":"N° Nota Fiscal","columns.4":"Valor da Nota","columns.5":"Data de Emissão","columns.6":"Setor","columns.7":"PIN","columns.8":"Data Desembaraço Sefaz","columns.9":"Data Limite de Vistoria","columns.10":"Qtde de dias Restantes P/ Vistoria","columns.11":"Qtde de Itens","fields.0":"ufRemetente","fields.1":"cnpjRemetenteFmt","fields.2":"razaoRemetente","fields.3":"numeroNf","fields.4":"totalNfe","fields.5":"dataEmissaoNfeFmt","fields.6":"descSetor","fields.7":"numeroPin","fields.8":"dataSelagemSefazFmt","fields.9":"dataLimiteVistoriaFmt","fields.10":"qtdDias","fields.11":"qtdItensSolicitacao","tipoOpcao":"0","page":"1","size":"1000000","exportarListagem":"true"}

        payload = ""
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "authorization": f"""Bearer {self.suframa.driver.execute_script("return window.sessionStorage.getItem('token');")}""",
            "frontguid": "b245529f-29da-8ffa-3e5e-f3f15795e1ae",
            "origin": "https://simnac.suframa.gov.br",
            "priority": "u=1, i",
            "referer": "https://simnac.suframa.gov.br/",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
        }
        response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
        self.suframa.fechar()
        if response.status_code != 200:
            raise Exception(f'Consulta confirmar recebimento status code {response.status_code}')
        return json.loads(response.text).get('items')

    @log_exceptions
    def pegar_data_emissao_mais_antiga_suframa(self, lista_itens:list[dict]) -> "datetime":
        mais_antiga = None

        for item in lista_itens:
            data_emissao = datetime.strptime(item.get("dataEmissaoNfe"), "%Y-%m-%dT%H:%M:%S")
            
            if mais_antiga is None or data_emissao < mais_antiga:
                mais_antiga = data_emissao
        
        return mais_antiga
    
    @log_exceptions
    def conciliar_itens(self, itens_suframa:list[dict], itens_fluig:list[dict]) -> list["DataFrame"]:
        # colunas suframa match cnpjRemetenteFmt e numeroNf
        data_frame_suframa = pd.DataFrame.from_dict(itens_suframa)
        # Colunas fluig match CNPJFORNECEDOR e NR_DOCUMENTO
        data_frame_fluig = pd.DataFrame.from_dict(itens_fluig)
        
        for coluna in COLUNAS_MERGE_SUFRAMA:
            data_frame_suframa[coluna] = data_frame_suframa[coluna].astype(str).str.strip()
        for coluna in COLUNAS_MERGE_FLUIG:
            data_frame_fluig[coluna] = data_frame_fluig[coluna].astype(str).str.strip()

        data_frame_suframa['numeroNf'] = data_frame_suframa['numeroNf'].astype(str)
        data_frame_fluig['cnpjRemetenteFmt'] = data_frame_suframa['cnpjRemetenteFmt'].astype(str)
        data_frame_fluig['numeroNf'] = data_frame_suframa['numeroNf'].astype(str)
        df_mesclado =  data_frame_suframa.merge(
            data_frame_fluig[COLUNAS_MERGE_FLUIG+COLUNAS_FLUIG_UTILIZADAS],
            left_on=COLUNAS_MERGE_SUFRAMA,
            right_on=COLUNAS_MERGE_FLUIG,
            how='left'
        )[COLUNAS_UTILIZADAS_NO_PROCESSO].fillna('')
        df_mesclado =  df_mesclado.apply(lambda x: x.astype(str).str.strip())
        df_mesclado['STATUS'] = df_mesclado['STATUS'].replace('', 'NAO ENCONTRADO')
        return df_mesclado 

    @log_exceptions
    def inserir_na_base(self, df_itens:list["DataFrame"]) -> None:
        print("")