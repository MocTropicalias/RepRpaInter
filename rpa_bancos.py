import os
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
import psycopg as pg
import json



#lendo .env
load_dotenv()

#Atribuindo configurações e setando variaveis
prod1 = os.getenv("prod1")
prod2 = os.getenv("prod2")
dev1 = os.getenv("dev1")
dev2 = os.getenv("dev2")

hoje = date.today()
timestampa = str(hoje)+"_"+str(datetime.now().strftime('%Hh%Mm%Ss'))
ontem = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
ontem_query = "current_date-1"

#Criando e abrindo arquivo de log
caminho = os.path.dirname(__file__)+"/logs/log_"+timestampa+".txt" 
log = open(caminho,"x")

#Criando função para escrever log1's
def escrevelog(texto='',pref="",cond="P"):
    if cond == "P":
        log.write("\n"+pref+" - "+str(datetime.now().strftime('%H:%M:%S'))+" - "+str(texto))
    elif cond == "E":
        log.write("\n\n"+pref+" ERRO! - "+str(datetime.now().strftime('%H:%M:%S'))+" - "+str(texto))
    elif cond == "S":
        log.write("\n\n"+("="*30)+"\n\n")
    elif cond == "L":
        log.write("\n\n\n"+pref+" - "+str(datetime.now().strftime('%H:%M:%S'))+" - "+str(texto))

log.write("Execução dia "+timestampa)


#Lendo arquivo de config
with open(os.path.dirname(__file__)+"/config/config.json", 'r') as file:
    configs = json.load(file)

#Lendo em quais ambientes o programa será executado
if configs['rodaPrd'] == "S" and configs['rodaDev'] == "S":
    ambs = ["prd","dev"]
    escrevelog("Executando nos ambientes de DEV e PRD")
elif configs['rodaPrd'] != "S" and configs['rodaDev'] == "S":
    ambs = ["dev"]
    escrevelog("Executando no ambiente de DEV")
elif configs['rodaPrd'] == "S" and configs['rodaDev'] != "S":
    ambs = ["prd"]
    escrevelog("Executando no ambiente de PRD")
else:
    escrevelog("Está escrito no arquivo de config que nenhum ambiente será executado.")
    escrevelog("Encerrando programa")
    quit()

escrevelog(cond="S")

#O loop roda 2 vezes, a primeira no ambiente produtivo e a segunda no ambiente de dev
for amb in ambs:
    if amb == "prd":
        database1 = prod1
        database2 = prod2
        database1txt = "prod1"
        database2txt = "prod2"

        escrevelog("Processo no banco prd",pref=amb)
        print("Começando processo banco prd")
    else:
        database1 = dev1
        database2 = dev2
        database1txt = "dev1"
        database2txt = "dev2"

        escrevelog("Processo no banco dev",pref=amb)
        print("Começando processo banco dev")

    try:
        #criando conexões no banco
        escrevelog("Conectando no banco do 1°",pref=amb)
        conn1 = pg.connect(database1)
        escrevelog("Conectando no banco do 2°",pref=amb)
        conn2 = pg.connect(database2)
        cur1 = conn1.cursor()
        cur2 = conn2.cursor()

        #============================================================================================
        #Passando novos registros do banco do 1° para o banco do 2°
        #============================================================================================

        escrevelog("Passando novos registros do banco do 1° para o banco do 2°",pref=amb, cond="L")


        # Obtendo dados da tabela tb_cor_mascote do banco do 1° e passando para o banco do 2°
        escrevelog("Obtendo dados da tabela tb_cor_mascote",pref=amb, cond="L")
        print(database1txt+"---->"+database2txt+" // Insert tb_cor_mascote")

        cur1.execute('select pk_int_id_cor_mascote, text_fundo, text_secundaria, text_primaria, deletedAt from tb_cor_mascote where createdAt = '+ontem_query)
        cores = cur1.fetchall()

        #Verificando se algum novo registro foi encontrado
        if len(cores) > 0:
            #Inserindo todos os registros encontrados no banco do 2°
            escrevelog("Inserindo registros", pref=amb)

            query = "INSERT INTO tb_cor_araci(pk_int_id_cor_araci, var_fundo, var_secundaria, var_primaria, deletedAt) values "
            for i in cores:
                query = query+"("+str(i[0])+",'"+str(i[1])+"','"+str(i[2])+"','"+str(i[3])+"','"+str(i[4])+"'),"
            query = ((query[:-1]).replace("'None'","null")).replace("None","null")
            cur2.execute(query)
            conn2.commit()

            escrevelog("Registros inseridos com sucesso", pref=amb)
        else:
            escrevelog("Nenhum novo registro encontrado",pref=amb)

        #===========================================================================================

        #Obtendo dados da tabela tb_evento do banco do 1° e passando para o banco do 2°
        escrevelog("Obtendo dados da tabela tb_evento",pref=amb, cond="L")
        print(database1txt+"---->"+database2txt+" // Insert tb_evento")

        cur1.execute('select dt_inicio, dt_final, var_nome, var_local, num_preco_ticket, pk_int_id_evento, fk_int_id_usuario, deletedat, var_imagem, var_descricao from tb_evento where createdAt = '+ontem_query)
        eventos = cur1.fetchall()

        #Verificando se algum novo registro foi encontrado
        if len(eventos) > 0:
            #Inserindo todos os registros encontrados no banco do 2°
            escrevelog("Inserindo registros", pref=amb)

            query = "INSERT INTO tb_evento(dt_data_inicio, dt_data_final, var_nome, var_local, float_preco_ticket, pk_int_id_evento, fk_int_id_usuario, deletedat, var_imagem, var_descricao) values "
            for i in eventos:
                query = query+"('"+str(i[0])+"','"+str(i[1])+"','"+str(i[2])+"','"+str(i[3])+"',"+str(i[4])+","+str(i[5])+","+str(i[6])+",'"+str(i[7])+"','"+str(i[8])+"','"+str(i[9])+"'),"
            query = ((query[:-1]).replace("'None'","null")).replace("None","null")
            cur2.execute(query)
            conn2.commit()

            escrevelog("Registros inseridos com sucesso", pref=amb)
        else:
            escrevelog("Nenhum novo registro encontrado",pref=amb)

        # #===========================================================================================

        #Obtendo dados da tabela tb_barraca do banco do 1° e passando para o banco do 2°
        escrevelog("Obtendo dados da tabela tb_barraca",pref=amb, cond="L")
        print(database1txt+"---->"+database2txt+" // Insert tb_barraca")

        cur1.execute('select pk_int_id_barraca, var_nome, fk_int_id_evento, deletedat from tb_barraca where createdAt = '+ontem_query)
        barracas = cur1.fetchall()

        #Verificando se algum novo registro foi encontrado
        if len(barracas) > 0:
            #Inserindo todos os registros encontrados no banco do 2°
            escrevelog("Inserindo registros", pref=amb)

            query = "INSERT INTO tb_barraca(pk_int_id_barraca, var_nome, fk_int_id_evento, deletedAt) values "
            for i in barracas:
                query = query+"("+str(i[0])+",'"+str(i[1])+"',"+str(i[2])+",'"+str(i[3])+"'),"
            query = ((query[:-1]).replace("'None'","null")).replace("None","null")
            cur2.execute(query)
            conn2.commit()

            escrevelog("Registros inseridos com sucesso", pref=amb)
        else:
            escrevelog("Nenhum novo registro encontrado",pref=amb)
        
        escrevelog(cond="S")
        # ============================================================================================
        # Passando registros recentemente alterados do banco do 1° para o banco do 2°
        # ============================================================================================

        escrevelog("Passando registros alterados no banco do 1° para o banco do 2°",pref=amb)


        #Obtendo dados atualizados da tabela tb_cor_mascote do banco do 1° e passando para o banco do 2°
        escrevelog("Obtendo dados da tabela tb_cor_mascote",pref=amb, cond="L")
        print(database1txt+"---->"+database2txt+" // Update tb_cor_mascote")


        cur1.execute('select pk_int_id_cor_mascote, text_fundo, text_secundaria, text_primaria, deletedAt from tb_cor_mascote where tb_cor_mascote.updateat = '+ontem_query+' order by pk_int_id_cor_mascote')
        cores = cur1.fetchall()

        #Verificando se algum novo registro foi encontrado
        if len(cores) > 0:
            escrevelog("Obtendo os registros equivalentes do banco do 2°",pref=amb)

            lista_cols = ["var_primaria","var_secundaria","var_fundo","deletedAt"]
            lista_mods = []
            querySelect = "select var_fundo, var_secundaria, var_primaria, deletedAt from tb_cor_araci where pk_int_id_cor_araci in ("
            for i in cores:
                querySelect = querySelect+str(i[0])+","
            querySelect = (querySelect[:-1])+") order by pk_int_id_cor_araci"
            cur2.execute(querySelect)
            cores2 = cur2.fetchall()

            escrevelog("Comparando Registros e salvando alterações a serem feitas",pref=amb)

            for i in range(0,len(cores2)):
                for c in range(0,len(cores2[i])):
                    if cores2[i][c] != cores[i][c+1]:
                        lista_mods.append([lista_cols[c],cores[i][c+1],cores2[i][0]])
            
            escrevelog("Realizando alterações",pref=amb)
            for i in lista_mods:
                query = (("update tb_cor_araci set "+str(i[0])+" = '"+str(i[1])+"' where pk_int_id_cor_araci = "+str(i[2])).replace("'None'","null")).replace("None","null")
                cur2.execute(query)
            conn2.commit()
            escrevelog("Alterações realizadas com sucesso",pref=amb)
        else:
            escrevelog("Nenhum dado alterado encontrado",pref=amb)

        #===========================================================================================

        # Obtendo dados atualizados da tabela tb_evento do banco do 1° e passando para o banco do 2°
        escrevelog("Obtendo dados da tabela tb_evento",pref=amb, cond="L")
        print(database1txt+"---->"+database2txt+" // Update evento")

    
        cur1.execute('select pk_int_id_evento, dt_inicio, dt_final, var_nome, var_local, num_preco_ticket, deletedat, fk_int_id_usuario, var_imagem, var_descricao from tb_evento where tb_evento.updateat ='+ontem_query+' order by pk_int_id_evento')
        eventos = cur1.fetchall()

        #Verificando se algum novo registro foi encontrado
        if len(eventos) > 0:
            escrevelog("Obtendo os registros equivalentes do banco do 2°",pref=amb)


            lista_cols = ["dt_data_inicio", "dt_data_final", "var_nome", "var_local", "float_preco_ticket", "deletedat", "fk_int_id_usuario", "var_imagem","var_descricao"]
            lista_mods = []
            querySelect = "select dt_data_inicio, dt_data_final, var_nome, var_local, float_preco_ticket, deletedat, fk_int_id_usuario, var_imagem, var_descricao from tb_evento where pk_int_id_evento in ("
            for i in eventos:
                querySelect = querySelect+str(i[0])+","
            querySelect = (querySelect[:-1])+") order by pk_int_id_evento"
            cur2.execute(querySelect)
            eventos2 = cur2.fetchall()


            escrevelog("Comparando Registros e salvando alterações a serem feitas",pref=amb)

            for i in range(0,len(eventos2)):
                for c in range(0,len(eventos2[i])):
                    if eventos2[i][c] != eventos[i][c+1]:
                        if c in [4,6]:
                            lista_mods.append([lista_cols[c],eventos[i][c+1],eventos2[i][0]])
                        else:
                            lista_mods.append([lista_cols[c],"'"+str(eventos[i][c+1])+"'",eventos2[i][0]])
            
            escrevelog("Realizando alterações",pref=amb)
            for i in lista_mods:
                query = (("update tb_evento set "+str(i[0])+" = "+str(i[1])+" where pk_int_id_evento = "+str(i[2])).replace("'None'","null")).replace("None","null")
                cur2.execute(query)
            conn2.commit()
            escrevelog("Alterações realizadas com sucesso",pref=amb)
        else:
            escrevelog("Nenhum dado alterado encontrado",pref=amb)

        #===========================================================================================

        #Obtendo dados atualizados da tabela tb_barraca do banco do 1° e passando para o banco do 2°
        escrevelog("Obtendo dados da tabela tb_barraca",pref=amb, cond="L")
        print(database1txt+"---->"+database2txt+" // Update tb_barraca")


        cur1.execute('select pk_int_id_barraca, var_nome, deletedat, fk_int_id_evento from tb_barraca where updateat = '+ontem_query+' order by pk_int_id_barraca')
        barracas = cur1.fetchall()

        #Verificando se algum novo registro foi encontrado
        if len(barracas) > 0:
            escrevelog("Obtendo os registros equivalentes do banco do 2°",pref=amb)


            lista_cols = ["var_nome", "deletedat", "fk_int_id_evento"]
            lista_mods = []
            querySelect = "select var_nome, deletedat, fk_int_id_evento from tb_barraca where pk_int_id_barraca in ("
            for i in barracas:
                querySelect = querySelect+str(i[0])+","
            querySelect = (querySelect[:-1])+") order by pk_int_id_barraca"
            cur2.execute(querySelect)
            barracas2 = cur2.fetchall()


            escrevelog("Comparando Registros e salvando alterações a serem feitas",pref=amb)

            for i in range(0,len(barracas2)):
                for c in range(0,len(barracas2[i])):
                    if barracas2[i][c] != barracas[i][c+1]:
                        if c == 2:
                            lista_mods.append([lista_cols[c],barracas[i][c+1],barracas2[i][0]])
                        else:
                            lista_mods.append([lista_cols[c],"'"+str(barracas[i][c+1])+"'",barracas2[i][0]])
            
            escrevelog("Realizando alterações",pref=amb)
            for i in lista_mods:
                query = (("update tb_barraca set "+str(i[0])+" = "+str(i[1])+" where pk_int_id_barraca = "+str(i[2])).replace("'None'","null")).replace("None","null")
                cur2.execute(query)
            conn2.commit()
            escrevelog("Alterações realizadas com sucesso",pref=amb)
        else:
            escrevelog("Nenhum dado alterado encontrado",pref=amb)

        
        #============================================================================================
        #Passando novos registros do banco do 2° para o banco do 1°
        #============================================================================================
        escrevelog(cond="S")
        escrevelog("Passando novos registros do banco do 2° para o banco do 1°",pref=amb, cond="L")

        # Obtendo dados da tabela tb_endereco do banco do 2° e passando para o banco do 1°
        escrevelog("Obtendo dados da tabela tb_endereco",pref=amb, cond="L")
        print(database2txt+"---->"+database1txt+" // Insert tb_endereco")

        cur2.execute('select e.pk_int_id_endereco, e.var_cep, es.var_estado, e.var_rua, e.var_cidade, e.var_complemento, e.int_num_casa, e.deletedat from tb_endereco e, tb_estado es where e.createdAt = '+ontem_query+' and fk_int_id_estado = pk_int_id_estado')
        enderecos = cur2.fetchall()

        #Verificando se algum novo registro foi encontrado
        if len(enderecos) > 0:
            #Inserindo todos os registros encontrados no banco do 1°
            escrevelog("Inserindo registros", pref=amb)

            query = "insert into tb_endereco(pk_int_id_endereco, var_cep, var_estado, var_rua, var_cidade, var_complemento, int_num_casa, deletedat, createdat) values "
            for i in enderecos:
                query = query+"("+str(i[0])+",'"+str(i[1])+"','"+str(i[2])+"','"+str(i[3])+"','"+str(i[4])+"','"+str(i[5])+"',"+str(i[6])+",'"+str(i[7])+"',current_date),"
            query = ((query[:-1]).replace("'None'","null")).replace("None","null")
            cur1.execute(query)
            conn1.commit()

            escrevelog("Registros inseridos com sucesso", pref=amb)
        else:
            escrevelog("Nenhum novo registro encontrado",pref=amb)


        #===========================================================================================


        # Obtendo dados da tabela tb_usuario do banco do 2° e passando para o banco do 1°
        escrevelog("Obtendo dados da tabela tb_usuario",pref=amb, cond="L")
        print(database2txt+"---->"+database1txt+" // Insert tb_usuario")

        cur2.execute('select pk_int_id_usuario, var_foto, var_email, var_senha, var_user_name, dt_nascimento, var_descricao_usuario, var_cpf, var_nome, deletedAt, fk_id_endereco, var_role from tb_usuario where createdAt = '+ontem_query)
        usuarios = cur2.fetchall()

        #Verificando se algum novo registro foi encontrado
        if len(usuarios) > 0:
            #Inserindo todos os registros encontrados no banco do 1°
            escrevelog("Inserindo registros", pref=amb)

            query = "insert into tb_usuario(pk_int_id_usuario, text_foto, var_email, var_senha, var_user_name, dt_nascimento, var_descricao_usuario, var_cpf, var_nome, deletedAt, fk_int_id_endereco, var_role, createdAt) values "
            for i in usuarios:
                query = query+"("+str(i[0])+",'"+str(i[1])+"','"+str(i[2])+"','"+str(i[3])+"','"+str(i[4])+"','"+str(i[5])+"','"+str(i[6])+"','"+str(i[7])+"','"+str(i[8])+"','"+str(i[9])+"','"+str(i[10])+"','"+str(i[11])+"',current_date),"
            query = ((query[:-1]).replace("'None'","null")).replace("None","null")
            cur1.execute(query)
            conn1.commit()

            escrevelog("Registros inseridos com sucesso", pref=amb)
        else:
            escrevelog("Nenhum novo registro encontrado",pref=amb)
        

        #===========================================================================================
        

        # Obtendo dados da tabela tb_mascote do banco do 2° e passando para o banco do 1°
        escrevelog("Obtendo dados da tabela tb_mascote",pref=amb, cond="L")
        print(database2txt+"---->"+database1txt+" // Insert tb_mascote")

        cur2.execute('select pk_int_id_mascote, var_nome, deletedAt, fk_int_id_cor_araci, fk_int_id_usuario from tb_mascote where createdAt = '+ontem_query)
        mascotes = cur2.fetchall()

        #Verificando se algum novo registro foi encontrado
        if len(mascotes) > 0:
            #Inserindo todos os registros encontrados no banco do 1°
            escrevelog("Inserindo registros", pref=amb)

            query = "insert into tb_mascote(pk_int_id_mascote, var_nome, deletedAt, fk_int_id_cor_mascote, fk_int_id_usuario, createdAt) values "
            for i in mascotes:
                query = query+"("+str(i[0])+",'"+str(i[1])+"','"+str(i[2])+"',"+str(i[3])+","+str(i[4])+",current_date),"
            query = ((query[:-1]).replace("'None'","null")).replace("None","null")
            cur1.execute(query)
            conn1.commit()

            escrevelog("Registros inseridos com sucesso", pref=amb)
        else:
            escrevelog("Nenhum novo registro encontrado",pref=amb)


        #===========================================================================================
        

        # Obtendo dados da tabela tb_venda_anuncio do banco do 2° e passando para o banco do 1°
        escrevelog("Obtendo dados da tabela tb_venda_anuncio",pref=amb, cond="L")
        print(database2txt+"---->"+database1txt+" // Insert tb_venda_anuncio")

        cur2.execute('select a.var_nota_fiscal, a.dt_data, a.num_valor, a.var_produto, a.int_quantidade, a.pk_int_id_venda_anuncio, s.var_status_venda, a.deletedAt, a.createdAt, a.fk_int_id_usuario from tb_venda_anuncio a, tb_status_venda s where a.fk_int_id_status_venda = s.pk_int_id_status_venda and a.createdat = '+ontem_query)
        anuncios = cur2.fetchall()

        #Verificando se algum novo registro foi encontrado
        if len(anuncios) > 0:
            #Inserindo todos os registros encontrados no banco do 1°
            escrevelog("Inserindo registros", pref=amb)

            query = "insert into tb_anuncio(var_nota_fiscal, dt_data, num_valor, var_produto, int_quantidade, pk_int_id_anuncio, var_status_venda, deletedAt, createdAt, fk_int_id_usuario) values "
            for i in anuncios:
                query = query+"('"+str(i[0])+"','"+str(i[1])+"',"+str(i[2])+",'"+str(i[3])+"',"+str(i[4])+","+str(i[5])+",'"+str(i[6])+"','"+str(i[7])+"',current_date"+","+str(i[9])+"),"
            query = ((query[:-1]).replace("'None'","null")).replace("None","null")
            cur1.execute(query)
            conn1.commit()

            escrevelog("Registros inseridos com sucesso", pref=amb)
        else:
            escrevelog("Nenhum novo registro encontrado",pref=amb)
        
        
        #===========================================================================================
        

        # Obtendo dados da tabela tb_follow do banco do 2° e passando para o banco do 1°
        escrevelog("Obtendo dados da tabela tb_follow",pref=amb, cond="L")
        print(database2txt+"---->"+database1txt+" // Insert tb_follow")

        cur2.execute('select pk_int_id_follow, fk_int_id_seguidor, fk_int_id_seguido, deletedAt, createdAt from tb_follow where createdat = '+ontem_query)
        follows = cur2.fetchall()

        #Verificando se algum novo registro foi encontrado
        if len(follows) > 0:
            #Inserindo todos os registros encontrados no banco do 1°
            escrevelog("Inserindo registros", pref=amb)

            query = "insert into tb_follow(pk_int_id_follow, fk_int_id_seguidor, fk_int_id_seguindo, deletedat, createdat, dt_data) values "
            for i in follows:
                query = query+"("+str(i[0])+","+str(i[1])+","+str(i[2])+",'"+str(i[3])+"',current_date,'"+str(i[4])+"'),"
            query = ((query[:-1]).replace("'None'","null")).replace("None","null")
            cur1.execute(query)
            conn1.commit()

            escrevelog("Registros inseridos com sucesso", pref=amb)
        else:
            escrevelog("Nenhum novo registro encontrado",pref=amb)

    
    #===========================================================================================
        

        # Obtendo dados da tabela tb_ticket do banco do 2° e passando para o banco do 1°
        escrevelog("Obtendo dados da tabela tb_ticket",pref=amb, cond="L")
        print(database2txt+"---->"+database1txt+" // Insert tb_ticket")

        cur2.execute('select pk_int_id_ticket, fk_int_id_usuario, fk_int_id_evento, int_quant, deletedat from tb_ticket where createdAt = '+ontem_query)
        ticket = cur2.fetchall()

        #Verificando se algum novo registro foi encontrado
        if len(ticket) > 0:
            #Inserindo todos os registros encontrados no banco do 1°
            escrevelog("Inserindo registros", pref=amb)

            query = "insert into tb_ticket(pk_int_id_ticket, fk_int_id_usuario, fk_int_id_evento, int_quant, deletedat, createdat) values "
            for i in ticket:
                query = query+"("+str(i[0])+","+str(i[1])+","+str(i[2])+","+str(i[3])+",'"+str(i[4])+"',current_date),"
            query = ((query[:-1]).replace("'None'","null")).replace("None","null")
            cur1.execute(query)
            conn1.commit()

            escrevelog("Registros inseridos com sucesso", pref=amb)
        else:
            escrevelog("Nenhum novo registro encontrado",pref=amb)


    #===========================================================================================
        

        # Obtendo dados da tabela tb_venda_evento do banco do 2° e passando para o banco do 1°
        escrevelog("Obtendo dados da tabela tb_venda_evento",pref=amb, cond="L")
        print(database2txt+"---->"+database1txt+" // Insert tb_venda_evento")

        cur2.execute('select pk_int_id_venda_evento, fk_int_id_usuario, fk_int_id_barraca, num_valor, deletedat, createdat from tb_venda_evento where createdAt = '+ontem_query)
        vendas_evento = cur2.fetchall()

        #Verificando se algum novo registro foi encontrado
        if len(vendas_evento) > 0:
            #Inserindo todos os registros encontrados no banco do 1°
            escrevelog("Inserindo registros", pref=amb)

            query = "insert into tb_venda_evento(pk_int_id_venda_evento, fk_int_id_usuario, fk_int_id_barraca, num_valor, deletedat, createdat) values "
            for i in vendas_evento:
                query = query+"("+str(i[0])+","+str(i[1])+","+str(i[2])+","+str(i[3])+",'"+str(i[4])+"',current_date),"
            query = ((query[:-1]).replace("'None'","null")).replace("None","null")
            cur1.execute(query)
            conn1.commit()

            escrevelog("Registros inseridos com sucesso", pref=amb)
        else:
            escrevelog("Nenhum novo registro encontrado",pref=amb)
    

    #============================================================================================
    #Passando registros recentemente alterados do banco do 2° para o banco do 1°
    #============================================================================================
        escrevelog(cond="S")
        escrevelog("Passando registros alterados no banco do 1° para o banco do 2°",pref=amb)


        #Obtendo dados atualizados da tabela tb_endereco do banco do 2° e passando para o banco do 1°
        escrevelog("Obtendo dados da tabela tb_endereco",pref=amb, cond="L")
        print(database2txt+"---->"+database1txt+" // Update tb_endereco")


        cur2.execute('select e.pk_int_id_endereco, e.var_cep, es.var_estado, e.var_rua, e.var_cidade, e.var_complemento, e.int_num_casa, e.deletedat from tb_endereco e, tb_estado es where e.updatedAt = '+ontem_query+' and fk_int_id_estado = pk_int_id_estado order by e.pk_int_id_endereco')
        enderecos2 = cur2.fetchall()

        #Verificando se algum novo registro foi encontrado
        if len(enderecos2) > 0:
            escrevelog("Obtendo os registros equivalentes do banco do 1°",pref=amb)

            lista_cols = ["var_cep", "var_estado", "var_rua", "var_cidade", "var_complemento", "int_num_casa", "deletedat"]
            lista_mods = []
            querySelect = "select var_cep, var_estado, var_rua, var_cidade, var_complemento, int_num_casa, deletedat from tb_endereco where pk_int_id_endereco in ("
            for i in enderecos2:
                querySelect = querySelect+str(i[0])+","
            querySelect = (querySelect[:-1])+") order by pk_int_id_endereco"
            cur1.execute(querySelect)
            enderecos = cur1.fetchall()

            escrevelog("Comparando Registros e salvando alterações a serem feitas",pref=amb)

            for i in range(0,len(enderecos)):
                for c in range(0,len(enderecos[i])):
                    if enderecos[i][c] != enderecos2[i][c+1]:
                        if c == 5:
                            lista_mods.append([lista_cols[c],enderecos2[i][c+1],enderecos[i][0]])
                        else:
                            lista_mods.append([lista_cols[c],"'"+str(enderecos2[i][c+1])+"'",enderecos[i][0]])
            
            escrevelog("Realizando alterações",pref=amb)
            for i in lista_mods:
                query = (("update tb_endereco set "+str(i[0])+" = "+str(i[1])+" where pk_int_id_endereco = "+str(i[2])).replace("'None'","null")).replace("None","null")
                cur1.execute(query)
            conn1.commit()
            escrevelog("Alterações realizadas com sucesso",pref=amb)
        else:
            escrevelog("Nenhum dado alterado encontrado",pref=amb)


        #===========================================================================================


        #Obtendo dados atualizados da tabela tb_usuario do banco do 2° e passando para o banco do 1°
        escrevelog("Obtendo dados da tabela tb_usuario",pref=amb, cond="L")
        print(database2txt+"---->"+database1txt+" // Update tb_usuario")


        cur2.execute('select pk_int_id_usuario, var_foto, var_email, var_senha, var_user_name, dt_nascimento, var_descricao_usuario, var_cpf, var_nome, deletedAt, fk_id_endereco, var_role from tb_usuario where updatedat = '+ontem_query+' order by pk_int_id_usuario')
        usuarios2 = cur2.fetchall()

        #Verificando se algum novo registro foi encontrado
        if len(usuarios2) > 0:
            escrevelog("Obtendo os registros equivalentes do banco do 1°",pref=amb)

            lista_cols = ["text_foto", "var_email", "var_senha", "var_user_name", "dt_nascimento", "var_descricao_usuario", "var_cpf", "var_nome", "deletedAt", "fk_int_id_endereco","var_role"]
            lista_mods = []
            querySelect = "select text_foto, var_email, var_senha, var_user_name, dt_nascimento, var_descricao_usuario, var_cpf, var_nome, deletedAt, fk_int_id_endereco, var_role from tb_usuario where pk_int_id_usuario in ("
            for i in usuarios2:
                querySelect = querySelect+str(i[0])+","
            querySelect = (querySelect[:-1])+") order by pk_int_id_usuario"
            cur1.execute(querySelect)
            usuarios = cur1.fetchall()

            escrevelog("Comparando Registros e salvando alterações a serem feitas",pref=amb)

            for i in range(0,len(usuarios)):
                for c in range(0,len(usuarios[i])):
                    if usuarios[i][c] != usuarios2[i][c+1]:
                        if c == 9:
                            lista_mods.append([lista_cols[c],usuarios2[i][c+1],usuarios[i][0]])
                        else:
                            lista_mods.append([lista_cols[c],"'"+str(usuarios2[i][c+1])+"'",usuarios[i][0]])
            
            escrevelog("Realizando alterações",pref=amb)
            for i in lista_mods:
                query = (("update tb_usuario set "+str(i[0])+" = "+str(i[1])+" where pk_int_id_usuario = "+str(i[2])).replace("'None'","null")).replace("None","null")
                cur1.execute(query)
            conn1.commit()
            escrevelog("Alterações realizadas com sucesso",pref=amb)
        else:
            escrevelog("Nenhum dado alterado encontrado",pref=amb)

        
        #===========================================================================================


        #Obtendo dados atualizados da tabela tb_venda_anuncio do banco do 2° e passando para o banco do 1°
        escrevelog("Obtendo dados da tabela tb_venda_anuncio",pref=amb, cond="L")
        print(database2txt+"---->"+database1txt+" // Update tb_venda_anuncio")


        cur2.execute('select a.pk_int_id_venda_anuncio, a.var_nota_fiscal, a.dt_data, a.num_valor, a.var_produto, a.int_quantidade, s.var_status_venda, a.deletedAt, a.fk_int_id_usuario from tb_venda_anuncio a, tb_status_venda s where a.fk_int_id_status_venda = s.pk_int_id_status_venda and a.updatedat = '+ontem_query+' order by a.pk_int_id_venda_anuncio')
        anuncios2 = cur2.fetchall()

        #Verificando se algum novo registro foi encontrado
        if len(anuncios2) > 0:
            escrevelog("Obtendo os registros equivalentes do banco do 1°",pref=amb)

            lista_cols = ["var_nota_fiscal", "dt_data", "num_valor", "var_produto", "int_quantidade", "var_status_venda", "deletedAt", "fk_int_id_usuario"]
            lista_mods = []
            querySelect = "select var_nota_fiscal, dt_data, num_valor, var_produto, int_quantidade, var_status_venda, deletedAt, fk_int_id_usuario from tb_anuncio where pk_int_id_anuncio in ("
            for i in anuncios2:
                querySelect = querySelect+str(i[0])+","
            querySelect = (querySelect[:-1])+") order by pk_int_id_anuncio"
            cur1.execute(querySelect)
            anuncios = cur1.fetchall()

            escrevelog("Comparando Registros e salvando alterações a serem feitas",pref=amb)

            for i in range(0,len(anuncios)):
                for c in range(0,len(anuncios[i])):
                    if anuncios[i][c] != anuncios2[i][c+1]:
                        if c in [2,4,7]:
                            lista_mods.append([lista_cols[c],anuncios2[i][c+1],anuncios[i][0]])
                        else:
                            lista_mods.append([lista_cols[c],"'"+str(anuncios2[i][c+1])+"'",anuncios[i][0]])
            
            escrevelog("Realizando alterações",pref=amb)
            for i in lista_mods:
                query = (("update tb_anuncio set "+str(i[0])+" = "+str(i[1])+" where pk_int_id_anuncio = "+str(i[2])).replace("'None'","null")).replace("None","null")
                cur1.execute(query)
            conn1.commit()
            escrevelog("Alterações realizadas com sucesso",pref=amb)
        else:
            escrevelog("Nenhum dado alterado encontrado",pref=amb)
        


        #===========================================================================================


        #Obtendo dados atualizados da tabela tb_mascote do banco do 2° e passando para o banco do 1°
        escrevelog("Obtendo dados da tabela tb_mascote",pref=amb, cond="L")
        print(database2txt+"---->"+database1txt+" // Update tb_mascote")


        cur2.execute('select pk_int_id_mascote, fk_int_id_cor_araci, fk_int_id_usuario, var_nome, deletedat from tb_mascote where updatedat = '+ontem_query+' order by pk_int_id_mascote')
        mascotes2 = cur2.fetchall()

        #Verificando se algum novo registro foi encontrado
        if len(mascotes2) > 0:
            escrevelog("Obtendo os registros equivalentes do banco do 1°",pref=amb)

            lista_cols = ["fk_int_id_cor_mascote", "fk_int_id_usuario", "var_nome", "deletedAt"]
            lista_mods = []
            querySelect = "select fk_int_id_cor_mascote, fk_int_id_usuario, var_nome, deletedAt from tb_mascote where pk_int_id_mascote in ("
            for i in mascotes2:
                querySelect = querySelect+str(i[0])+","
            querySelect = (querySelect[:-1])+") order by pk_int_id_mascote"
            cur1.execute(querySelect)
            mascotes = cur1.fetchall()

            escrevelog("Comparando Registros e salvando alterações a serem feitas",pref=amb)

            for i in range(0,len(mascotes)):
                for c in range(0,len(mascotes[i])):
                    if mascotes[i][c] != mascotes2[i][c+1]:
                        if c in [0,1]:
                            lista_mods.append([lista_cols[c],mascotes2[i][c+1],mascotes[i][0]])
                        else:
                            lista_mods.append([lista_cols[c],"'"+str(mascotes2[i][c+1])+"'",mascotes[i][0]])
            
            escrevelog("Realizando alterações",pref=amb)
            for i in lista_mods:
                query = (("update tb_mascote set "+str(i[0])+" = "+str(i[1])+" where pk_int_id_mascote = "+str(i[2])).replace("'None'","null")).replace("None","null")
                cur1.execute(query)
            conn1.commit()
            escrevelog("Alterações realizadas com sucesso",pref=amb)
        else:
            escrevelog("Nenhum dado alterado encontrado",pref=amb)
        

        #===========================================================================================


        #Obtendo dados atualizados da tabela tb_follow do banco do 2° e passando para o banco do 1°
        escrevelog("Obtendo dados da tabela tb_follow",pref=amb, cond="L")
        print(database2txt+"---->"+database1txt+" // Update tb_follow")


        cur2.execute('select pk_int_id_follow, fk_int_id_seguidor, fk_int_id_seguido, deletedat from tb_follow where updatedat = '+ontem_query+' order by pk_int_id_follow')
        follows2 = cur2.fetchall()

        #Verificando se algum novo registro foi encontrado
        if len(follows2) > 0:
            escrevelog("Obtendo os registros equivalentes do banco do 1°",pref=amb)

            lista_cols = ["fk_int_id_seguidor", "fk_int_id_seguindo", "deletedAt"]
            lista_mods = []
            querySelect = "select fk_int_id_seguidor, fk_int_id_seguindo, deletedAt from tb_follow where pk_int_id_follow in ("
            for i in follows2:
                querySelect = querySelect+str(i[0])+","
            querySelect = (querySelect[:-1])+") order by pk_int_id_follow"
            cur1.execute(querySelect)
            follows = cur1.fetchall()

            escrevelog("Comparando Registros e salvando alterações a serem feitas",pref=amb)

            for i in range(0,len(follows)):
                for c in range(0,len(follows[i])):
                    if follows[i][c] != follows2[i][c+1]:
                        if c in [0,1]:
                            lista_mods.append([lista_cols[c],follows2[i][c+1],follows[i][0]])
                        else:
                            lista_mods.append([lista_cols[c],"'"+str(follows2[i][c+1])+"'",follows[i][0]])
            
            escrevelog("Realizando alterações",pref=amb)
            for i in lista_mods:
                query = (("update tb_follow set "+str(i[0])+" = "+str(i[1])+" where pk_int_id_follow = "+str(i[2])).replace("'None'","null")).replace("None","null")
                cur1.execute(query)
            conn1.commit()
            escrevelog("Alterações realizadas com sucesso",pref=amb)
        else:
            escrevelog("Nenhum dado alterado encontrado",pref=amb)

        
        #===========================================================================================


        #Obtendo dados atualizados da tabela tb_ticket do banco do 2° e passando para o banco do 1°
        escrevelog("Obtendo dados da tabela tb_ticket",pref=amb, cond="L")
        print(database2txt+"---->"+database1txt+" // Update tb_ticket")


        cur2.execute('select pk_int_id_ticket, fk_int_id_usuario, fk_int_id_evento, int_quant, deletedat from tb_ticket where updatedat = '+ontem_query+' order by pk_int_id_ticket')
        tickets2 = cur2.fetchall()

        #Verificando se algum novo registro foi encontrado
        if len(tickets2) > 0:
            escrevelog("Obtendo os registros equivalentes do banco do 1°",pref=amb)

            lista_cols = ["fk_int_id_usuario", "fk_int_id_evento", "int_quant", "deletedAt"]
            lista_mods = []
            querySelect = "select fk_int_id_usuario, fk_int_id_evento, int_quant, deletedat from tb_ticket where pk_int_id_ticket in ("
            for i in tickets2:
                querySelect = querySelect+str(i[0])+","
            querySelect = (querySelect[:-1])+") order by pk_int_id_ticket"
            cur1.execute(querySelect)
            tickets = cur1.fetchall()

            escrevelog("Comparando Registros e salvando alterações a serem feitas",pref=amb)

            for i in range(0,len(tickets)):
                for c in range(0,len(tickets[i])):
                    if tickets[i][c] != tickets2[i][c+1]:
                        if c in [0,1,2]:
                            lista_mods.append([lista_cols[c],tickets2[i][c+1],tickets[i][0]])
                        else:
                            lista_mods.append([lista_cols[c],"'"+str(tickets2[i][c+1])+"'",tickets[i][0]])
            
            escrevelog("Realizando alterações",pref=amb)
            for i in lista_mods:
                query = (("update tb_ticket set "+str(i[0])+" = "+str(i[1])+" where pk_int_id_ticket = "+str(i[2])).replace("'None'","null")).replace("None","null")
                cur1.execute(query)
            conn1.commit()
            escrevelog("Alterações realizadas com sucesso",pref=amb)
        else:
            escrevelog("Nenhum dado alterado encontrado",pref=amb)


        #===========================================================================================


        #Obtendo dados atualizados da tabela tb_venda_evento do banco do 2° e passando para o banco do 1°
        escrevelog("Obtendo dados da tabela tb_venda_evento",pref=amb, cond="L")
        print(database2txt+"---->"+database1txt+" // Update tb_venda_evento")


        cur2.execute('select pk_int_id_venda_evento, fk_int_id_barraca, fk_int_id_usuario, num_valor, deletedat from tb_venda_evento where updatedat = '+ontem_query+' order by pk_int_id_venda_evento')
        vendas_evento2 = cur2.fetchall()

        #Verificando se algum novo registro foi encontrado
        if len(vendas_evento2) > 0:
            escrevelog("Obtendo os registros equivalentes do banco do 1°",pref=amb)

            lista_cols = ["fk_int_id_barraca", "fk_int_id_usuario", "num_valor", "deletedAt"]
            lista_mods = []
            querySelect = "fk_int_id_barraca, fk_int_id_usuario, num_valor, deletedat where pk_int_id_venda_evento in ("
            for i in vendas_evento2:
                querySelect = querySelect+str(i[0])+","
            querySelect = (querySelect[:-1])+") order by pk_int_id_venda_evento"
            cur1.execute(querySelect)
            vendas_evento = cur1.fetchall()

            escrevelog("Comparando Registros e salvando alterações a serem feitas",pref=amb)

            for i in range(0,len(vendas_evento)):
                for c in range(0,len(vendas_evento[i])):
                    if vendas_evento[i][c] != vendas_evento2[i][c+1]:
                        if c in [0,1,2]:
                            lista_mods.append([lista_cols[c],vendas_evento2[i][c+1],vendas_evento[i][0]])
                        else:
                            lista_mods.append([lista_cols[c],"'"+str(vendas_evento2[i][c+1])+"'",vendas_evento[i][0]])
            
            escrevelog("Realizando alterações",pref=amb)
            for i in lista_mods:
                query = (("update tb_venda_evento set "+str(i[0])+" = "+str(i[1])+" where pk_int_id_venda_evento = "+str(i[2])).replace("'None'","null")).replace("None","null")
                cur1.execute(query)
            conn1.commit()
            escrevelog("Alterações realizadas com sucesso",pref=amb)
        else:
            escrevelog("Nenhum dado alterado encontrado",pref=amb)



    except (pg.Error) as error:
        print("Erro banco: ", error)
        escrevelog(error,pref=amb,cond="E")
    except (Exception) as error:
        print("Erro código python")
        escrevelog(error,pref=amb,cond="E")

    finally:
        if conn1:
            cur1.close()
            conn1.close()
            escrevelog("Conexão banco 1° encerrada",pref=amb,cond="L")
        if conn2:
            cur2.close()
            conn2.close()
            escrevelog("Conexão banco 2° encerrada",pref=amb)
        print("Conexões encerradas\n\n")
        escrevelog(cond="S")