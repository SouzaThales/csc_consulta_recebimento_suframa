import sys
from libs.exceptions import log_exceptions


class DBManager():


    def __init__(self, db:object):
        self.db = db
        
    @log_exceptions
    def pegar_filiais_para_processamento(self) -> None:
        return self.db.execute_query(f'''
                              SELECT 
                                CONTROLE.ID,
                                CONTROLE.CNPJ_FILIAL,
                                COFRE.DSCHAVE,
                                COFRE.DSVALORCRIPTOGRAFADO,
                                COFRE.USUARIO
                              FROM 
                                CSC_CONTROLE_RECEBIMENTO_MERCADORIAS AS CONTROLE 
                              INNER JOIN 
                                COFRECREDENCIAIS AS COFRE 
                              ON
                                COFRE.DSCOFRECREDENCIAL = CONTROLE.CNPJ_FILIAL 
                              WHERE
                                CONTROLE.FL_ATIVO = 1 
                              AND
                                COFRE.FLATIVO = 1 
                                ''')
    
    @log_exceptions
    def pegar_chaves_para_processar(self) -> list[dict]:
        return self.db.execute_query(f'''    
                                SELECT 
                                    *
                                FROM 
                                    CSC_DOWNLOAD_DANFES 
                                WHERE 
                                    FL_ATIVO = 1 
                                AND 
                                    ID_STATUS_PROCESSAMENTO = 15 
                                ;''')
    
    @log_exceptions
    def atualizar_download_na_base_de_dados(self, id_linha_na_base:int, caminho_download:str, tipo:str) -> None:
        return self.db.execute_query(f'''   
                                        UPDATE 
                                            CSC_DOWNLOAD_DANFES 
                                        SET
                                            CAMINHO_DOWNLOAD_{tipo.upper()} = '{caminho_download}',
                                            DT_ALTERACAO = GETDATE()
                                        WHERE 
                                            ID = {id_linha_na_base}
                                    ''')
        
    @log_exceptions
    def finalizar_processamento_na_base_de_dados(self, id_linha_na_base:int) -> None:
        danfe = self.db.execute_query(f'''   
                                        SELECT 
                                            CAMINHO_DOWNLOAD_PDF,
                                            CAMINHO_DOWNLOAD_XML
                                        FROM 
                                            CSC_DOWNLOAD_DANFES 
                                        WHERE 
                                            ID = {id_linha_na_base}
                                            
                                    ''')[0]
        
        if danfe.get('CAMINHO_DOWNLOAD_PDF') and danfe.get('CAMINHO_DOWNLOAD_XML'):
            self.db.execute_query(f'''   
                                        UPDATE 
                                            CSC_DOWNLOAD_DANFES 
                                        SET
                                            ID_STATUS_PROCESSAMENTO = 3,
                                            DT_ALTERACAO = GETDATE(),
                                            STATUS = 'Finalizado'
                                        WHERE 
                                            ID = {id_linha_na_base}
                                    ''')