from twilio.rest import Client
import pandas as pd
import mysql.connector

def conectar_banco():
    conexao = mysql.connector.connect(
        host='',
        user='',
        password='',
        database=''
    )
    cursor = conexao.cursor()
    print("Conectando ao banco de dados")
    return conexao, cursor

def buscar_ocorrencias(cursor):
    comando = f"SELECT ...
    
    cursor.execute(comando)
    print("Buscando infratores no banco de dados")
    return pd.DataFrame(cursor.fetchall())

def limpar_formato_telefone(telefone):
    if telefone and telefone[0] == '0':
        return telefone[1:]
    return telefone

def enviar_mensagem_whatsapp(client, telefone, mensagem, mensagem2):
    if telefone.strip() != '':
        message = client.messages.create(
            from_='whatsapp:',
            # from_= f'whatsapp:+55{telefone}', # nosso telefone
            body=mensagem,
            # to=f'whatsapp:+55{telefone}', # telefone do sindico
            to='whatsapp:'
            # to='whatsapp:'
        )
        message = client.messages.create(
            from_='whatsapp:',
            # from_= f'whatsapp:+55{telefone}', # nosso telefone
            body=mensagem2,
            # to=f'whatsapp:+55{telefone}', # telefone do sindico
            to='whatsapp:+'
            # to='whatsapp:'
        )
        print("Mensagem enviada para o telefone: ", telefone)

def enviar_mensagens(dataset, client):
    saudacao_emoji = "\U0001F44B"
    estrela_emoji = "\U0001F31F"
    maos_rezando_emoji = "\U0001F64F"

    lojas_desejadas = ['VN CONSOLAÇÃO', 'SETIN DOWNTOWN REPÚBLICA', 'VIBRA ESTAÇÃO CAPÃO REDONDO', 'MARSELHA', 'PITANGUEIRAS', 'ALTAVIS', 'SMART BREAK', 'SMART BREAK CDD', 'ALTAVIS', 'ECOLAB', 'TRILHAS DO BOSQUE', 'UNO']

    dataset = dataset.drop_duplicates(subset='Loja')

    for index, row in dataset.iterrows():
        loja = row['Loja'].upper()
        print("Verificando se existe infratores nas lojas:", ', '.join(lojas_desejadas))

        if loja in lojas_desejadas:
            telefone = limpar_formato_telefone(row['Telefone'])
            if pd.notna(telefone):
                mensagem = f'Olá! {saudacao_emoji}\nSou da equipe de Prevenção e Perdas da Smart Break. Já ouviu falar sobre incidentes de transação? Basicamente, são situações em que, por algum motivo, o pagamento de uma compra não é realizado com sucesso. Isso pode acontecer por um erro no processo de pagamento ou quando itens são levados sem o devido pagamento.\nNosso objetivo é simples: queremos melhorar continuamente a experiência dos condôninos em nossos minimercados. Como? Aumentando nosso faturamento ao reduzir esses incidentes. Isso significa mais investimentos em nossa infraestrutura, trazendo-nos mais perto de você com promoções, ativações e outras ações incríveis que enriquecem nossa comunidade.\nPrecisamos da sua ajuda! {maos_rezando_emoji}\nVocê encontrará abaixo um link para identificar os responsáveis por incidentes de transação no condomínio {loja}. Sua colaboração é essencial para nós.\nhttp://\nObrigado por fazer parte da nossa jornada!\nEquipe Smart Break  {estrela_emoji}\nDigite o codigo de acesso:'
                mensagem2=""
                enviar_mensagem_whatsapp(client, telefone, mensagem, mensagem2)
        else:
            print("Não existem infratores na loja", loja)

def main():
    conexao, cursor = conectar_banco()
    dataset = buscar_ocorrencias(cursor)
    cursor.close()
    conexao.close()

    if dataset.empty:
        print("Não existem ocorrências nas lojas desejadas.")
    else:
        novo_nome_colunas = ['Data', 'Loja', 'ID_LOJA', 'Sindico', 'Telefone']
        dataset.columns = novo_nome_colunas
        client = Client('', '')
        enviar_mensagens(dataset, client)

if __name__ == "__main__":
    main()
