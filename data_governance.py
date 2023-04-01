from pyspark.sql import functions as F
from cryptography.fernet import Fernet


class data_governence():
    def hash_column(self, df, column_name):
        """
        This will take dataframe and column name as input, result will be dataframe with additional column hashed_{columnname}
        Input:
        |name         |          
        |-------------|
        |         John|

        Output:
        |name        |hashed_name                                                     |
        |------------|----------------------------------------------------------------|
        |        John|a8cfcd74832004951b4408cdb0a5dbcd8c7e52d43f7fe244bf720582e05241da|

        """

        res_col = f'hashed_{column_name}'
        df = df.withColumn(res_col, F.sha2(F.col(column_name), 256) )
        return df
    
    def encrypt(self, text, key):
        """
        @Parameter -> Value to encrypt, Key generated using Fernet

        register below function
        encrypted_udf = udf(encrypt_user_id, StringType())  

        token = f.encrypt(b"secret message")
        print(token) --> b'gAAAAABkJuTN7Aj_xRW1mT8CF2a2bLh-vLEleZ8W-Dpi_4C8rtRwN8E8iB1lrqFbb6xrBIERiOMM7wJk6ReuxxuOu275OteolA=='
        """
        
        f = Fernet(key)
        plain_text_byte = bytes(text, encoding='utf-8')
        token = f.encrypt(plain_text_byte)
        text_decode =  token.decode('ascii')
        return  text_decode

    def decrypt(self, cipher_text, key):
        """
        @Parameter -> Value to decrypt , Key generated using Fernet
        Decrypt the encrypted value
        decrypted_udf = udf(decrypt_user_id, StringType())

        print(f.decrypt(token).decode()) --> secret message
        """
        
        f = Fernet(key)
        clear_val = f.decrypt(cipher_text.encode()).decode()
        return clear_val
    
