from libs.exceptions import log_exceptions


class DBManager():


    def __init__(self, db:object):
        self.db = db
        
    @log_exceptions
    def pegar_filiais_para_processamento(self) -> list[dict]:
        return self.db.execute_query('''
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
    def pegar_chamado_para_abrir(self, maquina:str, usuario:str, assumir_linha:bool=True) -> list[dict]:
        assumir = 1 if assumir_linha else 0
        return self.db.execute_query(f''' EXEC sp_csc_assumir_nota_recebimento_para_abrir_chamado '{maquina}', '{usuario}', {assumir} ''')
    
    @log_exceptions
    def atualizar_execucao_filial(self, id:int, tempo_execucao:float) -> list[dict]:
        self.db.execute_query(f'''
                              UPDATE 
                                CSC_CONTROLE_RECEBIMENTO_MERCADORIAS
                              SET 
                                DT_ULTIMA_EXECUCAO = GETDATE(),
                                TEMPO_EXECUCAO = {tempo_execucao}
                              WHERE
                                ID = {id} 
                                ''')
    
    @log_exceptions
    def pegar_notas_na_base(self) -> list[dict]:
        return self.db.execute_query(f'''   
                                        SELECT
                                            *
                                        FROM 
                                            CSC_NOTAS_AGUARDANDO_RECEBIMENTO 
                                        WHERE 
                                            FL_ATIVO = 1
                                    ''')

    @log_exceptions
    def inserir_notas_na_base(self, notas_para_abrir_chamados:list[dict]) -> None:
        notas_na_base = self.pegar_notas_na_base()
        
        queries = ''
        for infos_item in notas_para_abrir_chamados:
            chave_unica = infos_item.get('CNPJ_SEM_MASCARA')+'_'+infos_item.get('numeroNf')
            relacionados_na_base = [nota for nota in notas_na_base if nota.get('CHAVE_IDENTIFICADORA') == chave_unica]
            tem_dois_chamados_na_base = len(relacionados_na_base) == 2
            tem_na_base_e_esta_dentro_do_limite_vistoria = len(relacionados_na_base) > 0 and int(infos_item.get('qtdDias')) > 5

            if tem_dois_chamados_na_base or tem_na_base_e_esta_dentro_do_limite_vistoria:
                continue
    
            queries += f'''
                        INSERT INTO CSC_NOTAS_AGUARDANDO_RECEBIMENTO
                            (
                              ID_STATUS_PROCESSAMENTO, 
                              CHAVE_IDENTIFICADORA, 
                              STATUS_NOTA, 
                              NUMERO_CHAMADO_NOTA, 
                              QUANTIDADE_DIAS_VISTORIA,
                              CNPJ_FORNECEDOR,
                              NUMERO_NF,
                              CNPJ_FILIAL
                            ) 
                            VALUES (
                                24,
                                '{chave_unica}', 
                                '{infos_item.get('STATUS')}', 
                                '{infos_item.get('NUM_PROCES')}', 
                                {infos_item.get('qtdDias')}, 
                                '{infos_item.get('cnpjRemetenteFmt')}', 
                                '{infos_item.get('numeroNf')}', 
                                '{infos_item.get('CNPJ_FILIAL')}'
                            );
                        '''
            notas_na_base.append({'CHAVE_IDENTIFICADORA':chave_unica})
        if queries:
          self.db.execute_query(queries)