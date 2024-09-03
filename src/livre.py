import re
from datetime import datetime



current_server = r'\\sbcdf103\arquivos\SECRE\Sucon'
current_date = '28/05/2024 12:36'


def teste (rodada):
    user1 = {"Guilherme": [{'group': 'GC-SECRE-SUCON(RX)', 'access_level': 5}]}
    user2 = {"Guilherme": [{'group': 'GC-SECRE-SUCON-MASTER(F)', 'access_level': 6}]}
    user3 = {"Marcos": [{'group': 'GC-SECRE-SUCON-MASTER(F)', 'access_level': 6}]}
    if rodada == 1:
        return user1
    if rodada == 2:
        return user2
    if rodada == 3:
        return user3
    

users_data = {}
for i in range(1,4):
    user = teste(i)
    for chave, lista in user.items():
        if chave in users_data:
            users_data[chave].extend(lista)
        else:
            users_data[chave] = lista


server_data = {'Server': current_server,
                   'Date': current_date,
                   'Users': users_data}
print(server_data)