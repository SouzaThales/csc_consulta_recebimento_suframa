import time
from settings import *
from utils_aem import crypt_aes
from database.db import DataBase
from datetime import datetime, timedelta
from libs import consulta_recebimento, data_base_manager


def main():
    try:
        db = DataBase(CNN_STRING + crypt_aes.AESCipher().decrypt(DB_KEY, DB_ENC))
        daba_base = data_base_manager.DBManager(db)
        consulta = consulta_recebimento.ConsultaRecebimento() 
        credenciais_fluig = db.get_credenciais_no_cofre(18, 'FLUIG')
        infos_filiais = daba_base.pegar_filiais_para_processamento()

        for info_filial in infos_filiais:
            try:
                start_time = time.time()
                print(f"Processando {info_filial.get('USUARIO')}\n\tConsultando suframa...")
                notas_suframa = consulta.pegar_notas_suframa(info_filial.get('USUARIO'), crypt_aes.AESCipher().decrypt(info_filial.get('DSCHAVE'), info_filial.get('DSVALORCRIPTOGRAFADO')))
                if not notas_suframa:
                    print("\tNada nessa filial")
                else:
                    data_mais_antiga = consulta.pegar_data_emissao_mais_antiga_notas_suframa(notas_suframa)
                    print('\tSuframa consultado!\n\tConsultando fluig...')
                    solicitacoes_fluig = consulta.pegar_solicitacoes_fluig(credenciais_fluig.get('LOGIN'), crypt_aes.AESCipher().decrypt(credenciais_fluig.get('CHAVE'), credenciais_fluig.get('SENHA')), data_mais_antiga.strftime('%Y-%m-%d'), datetime.now().strftime('%Y-%m-%d'))
                    print('\tFluig consultado!\n\tConciliando informações...')
                    df_itens_conciliados = consulta.conciliar_informacoes(notas_suframa, solicitacoes_fluig, info_filial.get('CNPJ_FILIAL'))
                    df_notas_para_abrir_chamados = df_itens_conciliados.query("STATUS in ['FINALIZADA', 'CANCELADA', 'NAO ENCONTRADO']")
                    daba_base.inserir_notas_na_base(df_notas_para_abrir_chamados.to_dict(orient='records'))
                    print('\tNotas inseridas no banco de dados!')
                end_time = time.time() - start_time
                daba_base.atualizar_execucao_filial(info_filial.get('ID'), end_time)
                print(f'\tTempo de execução: {timedelta(seconds=int(end_time))}')
            except Exception as e:
                print(f'\t{e}')
                consulta.utils.kill_process_by_name_fast('Chrome.exe')
    except Exception as e:
        print(e)
    

if __name__ == "__main__":
    main()

