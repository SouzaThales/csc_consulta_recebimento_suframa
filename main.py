from settings import *
from datetime import datetime
from utils_aem import crypt_aes
from database.db import DataBase
from libs import consulta_recebimento, data_base_manager


def main():
    try:
        db = DataBase(CNN_STRING + crypt_aes.AESCipher().decrypt(DB_KEY, DB_ENC))
        daba_base = data_base_manager.DBManager(db)
        credenciais_fluig = db.get_credenciais_no_cofre(18, 'FLUIG')

        infos_filiais = daba_base.pegar_filiais_para_processamento()
        for info_filial in infos_filiais:
            try:
                consulta = consulta_recebimento.ConsultaRecebimento() 
                itens_suframa = consulta.pegar_itens_suframa(info_filial.get('USUARIO'), crypt_aes.AESCipher().decrypt(info_filial.get('DSCHAVE'), info_filial.get('DSVALORCRIPTOGRAFADO')))
                data_mais_antiga = consulta.pegar_data_emissao_mais_antiga_suframa(itens_suframa)
                itens_fluig = consulta.pegar_solicitacoes_fluig(credenciais_fluig.get('LOGIN'), crypt_aes.AESCipher().decrypt(credenciais_fluig.get('CHAVE'), credenciais_fluig.get('SENHA')), data_mais_antiga.strftime('%Y-%m-%d'), datetime.now().strftime('%Y-%m-%d'))
                df_itens_conciliados = consulta.conciliar_itens(itens_suframa, itens_fluig)
                df_abrir_chamados = df_itens_conciliados.query("STATUS in ['FINALIZADA', 'CANCELADA', 'NAO ENCONTRADO']")
                consulta.inserir_na_base(df_abrir_chamados)
            except Exception as e:
                print(e)
    
        
        print('')
    except Exception as e:
        print(e)
    

if __name__ == "__main__":
    main()