from settings import *
from utils_aem import crypt_aes
from database.db import DataBase
from libs import consulta_recebimento, data_base_manager

def main():
        db = DataBase(CNN_STRING + crypt_aes.AESCipher().decrypt(DB_KEY, DB_ENC))
        daba_base = data_base_manager.DBManager(db)
        consulta = consulta_recebimento.ConsultaRecebimento() 
        credenciais_fluig = db.get_credenciais_no_cofre(18, 'FLUIG')


if __name__ == "__main__":
    main()