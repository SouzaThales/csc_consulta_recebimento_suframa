import json
import requests
import pandas as pd
from settings import *
from utils_aem import utils
from datetime import datetime
from typing import TYPE_CHECKING
from libs.exceptions import log_exceptions
from selenium.webdriver.common.by import By
from sistemas.suframa import suframa_selenium
from selenium.webdriver.support.ui import WebDriverWait
from sistemas.fluig import fluig_request, fluig_selenium
from selenium.webdriver.support import expected_conditions as EC

if TYPE_CHECKING:
    from pandas import DataFrame

class ConsultaRecebimento():
    
    
    def __init__(self):
        self.url_abrir_chamado_revenda = 'https://fluig.fortbras.com.br:8444/portal/p/01/pageworkflowview?processID=falafort&coddep=1&codcel=4&codgrup=18&codatv=52'
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
    def pegar_notas_suframa(self, usuario, senha) -> list[dict]:
        suframa = suframa_selenium.SuframaSelenium()
        suframa.logar_suframa(usuario, senha)
        url = "https://appsimnac.suframa.gov.br/ConfirmarRecebimentoMercadoriaGrid"

        querystring = {"servico":"ConfirmarRecebimentoMercadoriaGrid","columns.0":"UF","columns.1":"CNPJ Remetente","columns.2":"Razão Social","columns.3":"N° Nota Fiscal","columns.4":"Valor da Nota","columns.5":"Data de Emissão","columns.6":"Setor","columns.7":"PIN","columns.8":"Data Desembaraço Sefaz","columns.9":"Data Limite de Vistoria","columns.10":"Qtde de dias Restantes P/ Vistoria","columns.11":"Qtde de Itens","fields.0":"ufRemetente","fields.1":"cnpjRemetenteFmt","fields.2":"razaoRemetente","fields.3":"numeroNf","fields.4":"totalNfe","fields.5":"dataEmissaoNfeFmt","fields.6":"descSetor","fields.7":"numeroPin","fields.8":"dataSelagemSefazFmt","fields.9":"dataLimiteVistoriaFmt","fields.10":"qtdDias","fields.11":"qtdItensSolicitacao","tipoOpcao":"0","page":"1","size":"1000000","exportarListagem":"true"}

        payload = ""
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "authorization": f"""Bearer {suframa.driver.execute_script("return window.sessionStorage.getItem('token');")}""",
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
        suframa.fechar()
        self.utils.kill_process_by_name_fast('Chrome.exe')
        if response.status_code != 200:
            raise Exception(f'Consulta confirmar recebimento status code {response.status_code}')
        return json.loads(response.text).get('items')

    @log_exceptions
    def pegar_data_emissao_mais_antiga_notas_suframa(self, lista_itens:list[dict]) -> "datetime":
        mais_antiga = None

        for item in lista_itens:
            data_emissao = datetime.strptime(item.get("dataEmissaoNfe"), "%Y-%m-%dT%H:%M:%S")
            
            if mais_antiga is None or data_emissao < mais_antiga:
                mais_antiga = data_emissao
        
        return mais_antiga
    
    @log_exceptions
    def conciliar_informacoes(self, notas_suframa:list[dict], solicitacoes_fluig:list[dict], cnpj_filial:str) -> list["DataFrame"]:
        data_frame_suframa = pd.DataFrame.from_dict(notas_suframa)
        data_frame_fluig = pd.DataFrame.from_dict(solicitacoes_fluig)
        
        for coluna in COLUNAS_MERGE_SUFRAMA:
            data_frame_suframa[coluna] = data_frame_suframa[coluna].astype(str).str.strip()
        for coluna in COLUNAS_MERGE_FLUIG:
            data_frame_fluig[coluna] = data_frame_fluig[coluna].astype(str).str.strip()
        
        data_frame_fluig['NUMERONOTAFISCAL'] = data_frame_fluig['NUMERONOTAFISCAL'].str.lstrip('0')
        data_frame_suframa['numeroNf'] = data_frame_suframa['numeroNf'].str.lstrip('0')
        data_frame_suframa['numeroNf'] = data_frame_suframa['numeroNf'].astype(str)
        data_frame_fluig['cnpjRemetenteFmt'] = data_frame_suframa['cnpjRemetenteFmt'].astype(str)
        data_frame_fluig['numeroNf'] = data_frame_suframa['numeroNf'].astype(str)
        # Deixar apenas uma linha por nf+forncedor sendo a do fluig mais recente
        data_frame_fluig['START_DATE'] = pd.to_datetime(data_frame_fluig['START_DATE'], format="%d/%m/%Y %H:%M", errors="coerce")
        data_frame_fluig = data_frame_fluig.loc[data_frame_fluig.groupby(COLUNAS_MERGE_FLUIG)['START_DATE'].idxmax()]

        # data_frame_fluig = data_frame_fluig.sort_values(COLUNAS_MERGE_FLUIG+['START_DATE'], ascending=[True, True, True])
        # data_frame_fluig = data_frame_fluig.drop_duplicates(subset=COLUNAS_MERGE_FLUIG, keep='last')

        df_mesclado = data_frame_suframa.merge(
            data_frame_fluig[COLUNAS_MERGE_FLUIG + COLUNAS_FLUIG_UTILIZADAS]
                .drop_duplicates(subset=COLUNAS_MERGE_FLUIG, keep='first'),
            left_on=COLUNAS_MERGE_SUFRAMA,
            right_on=COLUNAS_MERGE_FLUIG,
            how='left'
        )[COLUNAS_UTILIZADAS_NO_PROCESSO].fillna('')
        df_mesclado =  df_mesclado.apply(lambda x: x.astype(str).str.strip())
        df_mesclado['STATUS'] = df_mesclado['STATUS'].replace('', 'NAO ENCONTRADO')
        df_mesclado['CNPJ_SEM_MASCARA'] = df_mesclado['cnpjRemetenteFmt'].apply(lambda x: self.utils.remover_mascara(x))
        df_mesclado['CNPJ_FILIAL'] = cnpj_filial
        df_mesclado['qtdDias'] = data_frame_suframa['qtdDias'].astype(int)
        return df_mesclado             
    
    @log_exceptions
    def valida_corte_dias_vistoria(self, notas_suframa:list[dict]) -> list[dict]:
        if not notas_suframa:
            return []
        data_frame_suframa = pd.DataFrame.from_dict(notas_suframa)
        data_frame_suframa = data_frame_suframa.query(f'qtdDias <= {DIAS_VISTORIA_CORTE}')
        return data_frame_suframa.to_dict(orient='records')
                  
    @log_exceptions
    def montar_motivo_abertura_chamado(self, status_nota:str, numero_nf:str, numero_fluig:str, qnt_dias_vistoria:int) -> str:
        if status_nota != 'NAO ENCONTRADO':
            msg = f'''Nota fiscal {numero_nf}, Fluig {numero_fluig} - STATUS {status_nota} - QTD DE DIAS RESTANTE VISTORIA {qnt_dias_vistoria}'''
        else:
            msg = f'''Nota fiscal {numero_nf}, Fluig NÃO LOCALIZADO - QUANTIDADE DE DIAS RESTANTE VISTORIA {qnt_dias_vistoria}'''
        return msg             
    
    @log_exceptions
    def abrir_chamado_para_o_csc(self, usuario:str, senha:str, ambiente:str, cnpj_filial:str, cnpj_fornecedor:str, chave_acesso:str, razao_fornecedor:str, motivo_solicitacao:str) -> None:
        fluig = fluig_selenium.FluigSelenium(usuario, senha, ambiente)
        fluig.logar_fluig()
        fluig.driver.get(self.url_abrir_chamado_revenda)
        WebDriverWait(fluig.driver, 60).until(EC.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR, "#workflowView-cardViewer")))
        fluig.driver.find_element(By.CSS_SELECTOR, '#telefoneContato').send_keys('(99) 99999-9999')
        fluig.preencher_input_search_by_label('empresa', cnpj_filial)
        fluig.driver.find_element(By.CSS_SELECTOR, '#motivoSolicitacao').send_keys(motivo_solicitacao)
        fluig.driver.find_element(By.CSS_SELECTOR, '#tabDadosAdicionais').click()
        WebDriverWait(fluig.driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#chaveAcesso"))).send_keys(chave_acesso)
        fluig.driver.find_element(By.CSS_SELECTOR, '#cnpj').send_keys(cnpj_fornecedor)
        fluig.driver.find_element(By.CSS_SELECTOR, '#razaoSocial').send_keys(razao_fornecedor)
        fluig.driver.switch_to.default_content()
        fluig.driver.find_element(By.CSS_SELECTOR, '#send-process-button').click()
        WebDriverWait(fluig.driver, 120).until(EC.text_to_be_present_in_element((By.CSS_SELECTOR, "#message-page > div > div.title"), "iniciada com sucesso."))
        chamado = fluig.driver.find_element(By.CSS_SELECTOR, "#message-page > div > div.title > span > a").text.strip()
        self.utils.kill_process_by_name_fast('Chrome.exe')
        return chamado