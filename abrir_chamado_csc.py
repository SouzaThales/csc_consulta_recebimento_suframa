import os
from settings import *
from utils_aem import crypt_aes
from database.db import DataBase
from libs import consulta_recebimento, data_base_manager

def main() -> None:
    try:
        print('Inicio do processamento')
        db = DataBase(CNN_STRING + crypt_aes.AESCipher().decrypt(DB_KEY, DB_ENC))
        data_base = data_base_manager.DBManager(db)
        consulta = consulta_recebimento.ConsultaRecebimento() 
        credenciais_fluig = db.get_credenciais_no_cofre(0, 'FLUIG')
        lista_chamados_para_abrir = data_base.pegar_chamado_para_abrir(os.getlogin(), os.environ['COMPUTERNAME'])

        if not lista_chamados_para_abrir:
            print('Nada para fazer!')
            return

        for task in lista_chamados_para_abrir:
            try:
                print(f'Processando {task.get("CHAVE_IDENTIFICADORA")}...')
                print('\tAbrindo chamado...')
                usuario = credenciais_fluig.get('LOGIN')
                senha = crypt_aes.AESCipher().decrypt(credenciais_fluig.get('CHAVE'), credenciais_fluig.get('SENHA'))
                cnpj_filial = task.get('CNPJ_FILIAL')
                cnpj_fornecedor = task.get('CNPJ_FORNECEDOR')
                chave_nf = task.get('CHAVE_NF') if task.get('CHAVE_NF') else '00000000000000000000000000000000000000000000'
                razao_fornecedor = task.get('RAZAO_FORNECEDOR')
                status_nota = task.get('STATUS_NOTA')
                numero_nf = task.get('NUMERO_NF')
                numero_fluig = task.get('NUMERO_CHAMADO_NOTA')
                qnt_dias_vistoria = task.get('QUANTIDADE_DIAS_VISTORIA')
                motivo_solicitacao = consulta.montar_motivo_abertura_chamado(status_nota, numero_nf, numero_fluig, qnt_dias_vistoria)
                numero_chamado = consulta.abrir_chamado_para_o_csc(usuario, senha, AMBIENTE_FLUIG, cnpj_filial, cnpj_fornecedor, chave_nf, razao_fornecedor, motivo_solicitacao)
                print(f'\tChamado {numero_chamado} aberto!')
                data_base.atualizar_abertura_chamado(task.get('ID'), numero_chamado)
            except Exception as e:
                data_base.atualizar_erro_abertura_chamado(task.get('ID'), str(e))
                consulta.utils.kill_process_by_name_fast('Chrome.exe')
                print(f'{e}')
                
        print('Fim do processamento')
    except Exception as e:
        raise Exception(f'{e}')

if __name__ == "__main__":
    main()