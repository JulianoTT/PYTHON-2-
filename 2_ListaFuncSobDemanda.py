import requests
import os
import pandas as pd
import xml.etree.ElementTree as ET
import sys
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import create_engine, text
import pickle

def lista_func_sob_demanda():
    if getattr(sys, 'frozen', False):
        CURRENT_PATH = os.path.dirname(sys.executable)
    else:
        CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(CURRENT_PATH, 'DataFrame\df_funcSobDemanda.pickle')
    caminho_arquivo = os.path.join(CURRENT_PATH, 'Origem_Dados', 'Bases_Nuvem.xlsx')
    nome_aba = 'Bases_Nuvem'
    df1 = pd.read_excel(caminho_arquivo, sheet_name=nome_aba,header=0)
    engine1 = create_engine(f'sqlite:///{CURRENT_PATH}/Bancos/database.db')
    table_name = 'FUNCIONARIOS_NUVEM'
    data_final = pd.DataFrame() 
    for index, row in df1.iterrows():

        username = row['Usuario']
        password = row['senha']
        guiTenant = row['guiTenant']
        ambiente = row['Ambiente']
   
        url = 'https://prd-api1.lg.com.br/v2/servicodecontratodetrabalho'
        soap_action = 'lg.com.br/api/v2/ServicoDeContratoDeTrabalho/ConsultarListaPorDemanda'
        Total_Paginas = 1
        pagina_atual = 1
        
        while pagina_atual <= Total_Paginas:
            SOAPEnvelope = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:dto="lg.com.br/svc/dto" xmlns:v2="lg.com.br/api/v2" xmlns:v1="lg.com.br/api/dto/v1" xmlns:arr="http://schemas.microsoft.com/2003/10/Serialization/Arrays">
                                <soapenv:Header>
                                    <dto:LGAutenticacao>
                                        <dto:TokenUsuario>
                                            <dto:Senha>{password}</dto:Senha>
                                            <dto:Usuario>{username}</dto:Usuario>
                                            <dto:GuidTenant>{guiTenant}</dto:GuidTenant>
                                        </dto:TokenUsuario>
                                    </dto:LGAutenticacao>
                                    <dto:LGContextoAmbiente>
                                        <dto:Ambiente>{ambiente}</dto:Ambiente>
                                    </dto:LGContextoAmbiente>
                                </soapenv:Header>
                                <soapenv:Body>
                                    <v2:ConsultarListaPorDemanda>
                                        <v2:filtro>
                                            <v1:PaginaAtual>{pagina_atual}</v1:PaginaAtual>
                                            <v1:TiposDeSituacoes>
                                            <arr:int>1</arr:int>
                                            <arr:int>2</arr:int>
                                            <arr:int>3</arr:int>
                                            <arr:int>4</arr:int>
                                            <arr:int>5</arr:int>
                                            </v1:TiposDeSituacoes>
                                        </v2:filtro>
                                    </v2:ConsultarListaPorDemanda>
                                </soapenv:Body>
                                </soapenv:Envelope>'''
    
            options = {
                "Content-Type" : "text/xml;charset=UTF-8",
                'SOAPAction': soap_action
            }
            response = requests.post(url,data = SOAPEnvelope, headers = options,verify=False)
            root = ET.fromstring(response.text)
            total_paginas_element = root.find('.//{lg.com.br/api/dto/v1}TotalDePaginas')
            if total_paginas_element is not None:
                Total_Paginas = int(total_paginas_element.text)
            #print(Total_Paginas)
            def buscar_elemento_texto(elemento_pai, xpath):
                elemento = elemento_pai.find(xpath)
                
                if elemento is not None and elemento.get('{http://www.w3.org/2001/XMLSchema-instance}nil') != 'true':
                    return elemento.text
                else:
                    return None
                
        
     
            def buscar_elemento_texto(elemento, xpath):
                resultado = elemento.find(xpath)
                return resultado.text if resultado is not None else None

       
            def extrair_dados_para_dataframe(xml):
                data_frames = []
                root = ET.fromstring(xml)
                
                for contrato in root.findall('.//{lg.com.br/api/dto/v1}ContratoDeTrabalhoParcial'):
                    Tenant = row['tenant']
                    data_admissao = buscar_elemento_texto(contrato, '{lg.com.br/api/dto/v1}DataAdmissao')
                    data_inicio = buscar_elemento_texto(contrato, '{lg.com.br/api/dto/v1}DataInicioSituacaoAtual')
                    matricula = buscar_elemento_texto(contrato, '{lg.com.br/api/dto/v1}Matricula')
                    vinculo_empregaticio = buscar_elemento_texto(contrato, '{lg.com.br/api/dto/v1}VinculoEmpregaticio')
                    cpf = buscar_elemento_texto(contrato, './/{lg.com.br/api/dto/v1}Cpf')
                    data_nascimento = buscar_elemento_texto(contrato, './/{lg.com.br/api/dto/v1}DataDeNascimento')
                    nome = buscar_elemento_texto(contrato, './/{lg.com.br/api/dto/v1}Nome')
                    pessoa_id = buscar_elemento_texto(contrato, './/{lg.com.br/api/dto/v1}PessoaId')
                    codigo_cargo = buscar_elemento_texto(contrato, './/{lg.com.br/api/dto/v1}Cargo/{lg.com.br/api/dto/v1}Codigo')
                    descricao_cargo = buscar_elemento_texto(contrato, './/{lg.com.br/api/dto/v1}Cargo/{lg.com.br/api/dto/v1}Descricao')
                    codigo_cbo = buscar_elemento_texto(contrato, './/{lg.com.br/api/dto/v1}Cbo/{lg.com.br/api/dto/v1}Codigo')
                    descricao_cbo = buscar_elemento_texto(contrato, './/{lg.com.br/api/dto/v1}Cbo/{lg.com.br/api/dto/v1}Descricao')
                    codigo_centro_custo = buscar_elemento_texto(contrato, './/{lg.com.br/api/dto/v1}CentroDeCusto/{lg.com.br/api/dto/v1}Codigo')
                    descricao_centro_custo = buscar_elemento_texto(contrato, './/{lg.com.br/api/dto/v1}CentroDeCusto/{lg.com.br/api/dto/v1}Descricao')
                    codigo_empresa = buscar_elemento_texto(contrato, './/{lg.com.br/api/dto/v1}Empresa/{lg.com.br/api/dto/v1}Codigo')
                    descricao_empresa = buscar_elemento_texto(contrato, './/{lg.com.br/api/dto/v1}Empresa/{lg.com.br/api/dto/v1}Descricao')
                    codigo_estabelecimento = buscar_elemento_texto(contrato, './/{lg.com.br/api/dto/v1}Estabelecimento/{lg.com.br/api/dto/v1}Codigo')
                    descricao_estabelecimento = buscar_elemento_texto(contrato, './/{lg.com.br/api/dto/v1}Estabelecimento/{lg.com.br/api/dto/v1}Descricao')
                    codigo_situacao = buscar_elemento_texto(contrato, './/{lg.com.br/api/dto/v1}SituacaoDoColaborador/{lg.com.br/api/dto/v1}Codigo')
                    descricao_situacao = buscar_elemento_texto(contrato, './/{lg.com.br/api/dto/v1}SituacaoDoColaborador/{lg.com.br/api/dto/v1}Descricao')
                    codigo_sindicato = buscar_elemento_texto(contrato, './/{lg.com.br/api/dto/v1}Sindicato/{lg.com.br/api/dto/v1}Codigo')
                    descricao_sindicato = buscar_elemento_texto(contrato, './/{lg.com.br/api/dto/v1}Sindicato/{lg.com.br/api/dto/v1}Descricao')
                    categoria_colaborador = buscar_elemento_texto(contrato, '{lg.com.br/api/dto/v1}CategoriaDoColaborador')
                    marca_ponto = buscar_elemento_texto(contrato, '{lg.com.br/api/dto/v1}MarcaPonto')
                    tipo_ponto = buscar_elemento_texto(contrato, '{lg.com.br/api/dto/v1}TipoPonto')

                    data = {
                            'TENANT': Tenant,
                            'DATAADMISSAO': data_admissao,
                            'DATAINICIOSITUACAOATUAL': data_inicio,
                            'MATRICULA': matricula,
                            'VINCULOEMPREGATICIO': vinculo_empregaticio,
                            'CPF': cpf,
                            'DATADENASCIMENTO': data_nascimento,
                            'NOME': nome,
                            'PESSOAID': pessoa_id,
                            'CODIGOCARGO': codigo_cargo,
                            'DESCRICAOCARGO': descricao_cargo,
                            'CODIGOCBO': codigo_cbo,
                            'DESCRICAOCBO': descricao_cbo,
                            'CODIGOCENTROCUSTO': codigo_centro_custo,
                            'DESCRICAOCENTROCUSTO': descricao_centro_custo,
                            'CODIGOEMPRESA': codigo_empresa,
                            'DESCRICAOEMPRESA': descricao_empresa,
                            'CODIGOESTABELECIMENTO': codigo_estabelecimento,
                            'DESCRICAOESTABELECIMENTO': descricao_estabelecimento,
                            'CODIGOSITUACAO': codigo_situacao,
                            'DESCRICAOSITUACAO': descricao_situacao,
                            'CODIGOSINDICATO': codigo_sindicato,
                            'DESCRICAOSINDICATO': descricao_sindicato,
                            'CATEGORIADOCOLABORADOR': categoria_colaborador,
                            'MARCAPONTO': marca_ponto,
                            'TIPOPONTO': tipo_ponto
                        }

                    df = pd.DataFrame([data])
                    data_frames.append(df)   
                if data_frames:  
                    data_frame = pd.concat(data_frames)
                    return data_frame
                else:
                    return pd.DataFrame() 
            data_frame_temp = extrair_dados_para_dataframe(response.text)
            data_final = pd.concat([data_final, data_frame_temp])

            
              
            print(f'Página atual: {pagina_atual}')
            pagina_atual += 1
    with engine1.connect() as connection:
        result = connection.execute(text(f'DELETE FROM {table_name}'))
        connection.commit()
    engine1.dispose()

    print(data_final)
    try:
        data_final.to_sql(name='FUNCIONARIOS_NUVEM',con=engine1, if_exists='append', index=False)
        
        print('Inserção bem-sucedida.')

    except Exception as e:
        print(f'Erro ao inserir dados: {e}')
    engine1.dispose()
    print('Processamento concluído')
    with open(file_path, 'wb') as file:
        pickle.dump(data_final, file)
if __name__ == '__main__':
    lista_func_sob_demanda()