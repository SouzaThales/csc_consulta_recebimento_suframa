import os
from settings import *
from utils_aem import crypt_aes
from database.db import DataBase
from libs import consulta_recebimento, data_base_manager

def main():
        db = DataBase(CNN_STRING + crypt_aes.AESCipher().decrypt(DB_KEY, DB_ENC))
        daba_base = data_base_manager.DBManager(db)
        consulta = consulta_recebimento.ConsultaRecebimento() 
        credenciais_fluig = db.get_credenciais_no_cofre(18, 'FLUIG')
        lista_chamados_para_abrir = daba_base.pegar_chamado_para_abrir(os.getlogin(), os.environ['COMPUTERNAME'])

        for task in lista_chamados_para_abrir:
            # consulta.abrir_chamado_para_o_csc(credenciais_fluig.get('LOGIN'), crypt_aes.AESCipher().decrypt(credenciais_fluig.get('CHAVE'), credenciais_fluig.get('SENHA')), AMBIENTE_FLUIG)
            consulta.abrir_chamado_para_o_csc(credenciais_fluig.get('LOGIN'), 'Fort@0845', AMBIENTE_FLUIG, task)
if __name__ == "__main__":
    main()