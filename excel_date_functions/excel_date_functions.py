import os
import numpy    as np
import pandas   as pd
import tempfile

# Código criado por Milton Rocha
# https://medium.com/@milton-rocha

def feriados(override : bool = False) -> np.ndarray:

    """
    Função que faz o download dos feriados do site da Anbima e os compila em um formato a ser utilizado
    
      Resposta:
        np.array(feriados, dtype = 'datetime64[D]')
    """

    # O arquivo fornecido por download possui algumas marcas de fornecimento dos feriados que serão tratadas
    # O arquivo finaliza antes da linha que possui "Fonte: ANBIMA" como valor
    arq_temp = f'{tempfile.gettempdir()}/fer_anbima.parquet'

    # Checa se já existe um arquivo de feriados Anbima gerado na pasta temporária, caso não, cria o arquivo
    if not os.path.isfile(arq_temp) and not os.path.isfile('fer_anbima.parquet') and not override:
        feriados = pd.read_excel(r'https://www.anbima.com.br/feriados/arqs/feriados_nacionais.xls')
        feriados = feriados['Data'][ : feriados[feriados['Data'] == 'Fonte: ANBIMA'].index[0]].values # Acha a linha de footer
        feriados = pd.DataFrame({'Feriados ANBIMA' : feriados.astype('datetime64[D]')}) # Cria um dataframe com os dados
        feriados.to_parquet(arq_temp) # Exporta o DataFrame para .parquet
    else:
        feriados = pd.read_parquet(arq_temp) # Caso o arquivo já exista no diretório temporário, lê o .parquet
    
    # A função irá retornar um np.array contendo todas as datas de feriado disponíveis em formato 'datetime64[D]'
    return feriados.values.astype('datetime64[D]').flatten()
    
def networkdays(data_inicial, data_final) -> np.ndarray:
    """
    Função equivalente ao =NETWORKDAYS() do Excel
    Variáveis:
        data_inicial : str, list, np.ndarray, pd.Series
            É a data ou o conjunto de datas que serão utilizadas para cálculo do início dos dias úteis
        data_final   : str, list, np.ndarray, pd.Series
            É a data ou o conjunto de datas que serão utilizadas para cálculo do fim dos dias úteis
    Reposta:
        du  : np.ndarray
            Resposta do número de dias úteis entre os conjuntos de datas fornecidos
    """
    # Primeiramente criam-se dois np.array para cada uma das variáveis
    data_inicial, data_final = np.array(data_inicial).flatten(), np.array(data_final).flatten()
    #  Caso exista diferença entre o tamanho das variáveis, irá trabalhar com a que tem maior tamanho (v)
    # e repetirá a data da outra variável len(v) vezes
    if data_inicial.shape[0] != data_final.shape[0]:
        if data_inicial.shape[0] == 1:
            data_inicial = np.repeat(data_inicial, data_final.shape[0])
        elif data_final.shape[0] == 1:
            data_final = np.repeat(data_final, data_inicial.shape[0])
        elif data_inicial.shape[0] > 1 and data_final.shape[0] > 1:
          # Caso específico do usuário fornecendo as duas variáveis com mais de 1 elemento mas sem tamanho igual
            raise ValueError(f'O código não aceita data_inicial com tamanho > 1 [{data_inicial.shape[0]} elementos] ao mesmo tempo que data_final tem tamanho > 1 [{data_final.shape[0]} elementos]')
    
    # As variáveis de datas serão apenas um np.array convertido para o formato de 'datetime64[D]' (formato dos feriados)
    data_inicial, data_final = np.array(np.array([data_inicial, data_final])).astype('datetime64[D]')
    
    # A resposta é a contagem de dias úteis considerando a função anteriormente feita (feriados)
    return np.busday_count(data_inicial, data_final, holidays = feriados())
    
def workday(data_inicial, dias_uteis, rolagem = 'posterior'):
    """
    Função equivalente ao =WORKDAY() do Excel
      
    Variáveis:
        data_inicial : str, list, np.ndarray, pd.Series
            É a data ou o conjunto de datas que serão utilizadas para cálculo do início da data útil
        dias_uteis   : int
            Número de dias úteis para rolagem
        rolagem      : str
            Tipo de rolagem executada na data final
               
      Reposta:
      	data  : np.ndarray
            Resposta da data útil correspondente à rolagem feita
    """
    # Caso o usuário insira o tipo de rolagem, irá traduzir para o formato numpy
    if rolagem.lower() == 'posterior': rolagem = 'forward'
    if rolagem.lower() == 'anterior' : rolagem = 'backward'
    
    # Primeiramente cria-se um np.array para a variável e já define como 'datetime64[D]'
    data_inicial = np.array(data_inicial).flatten()
    data_inicial = data_inicial.astype('datetime64[D]')

    return np.busday_offset(data_inicial, dias_uteis, holidays = feriados(), roll = rolagem)
    
def work_dates(data_inicial, data_final, rolagem = None) -> np.ndarray:
    """
    Função para gerar dias úteis entre conjunto de datas especificado
    Variáveis:
        data_inicial : str, list, np.ndarray, pd.Series
            É a data ou o conjunto de datas que serão utilizadas para cálculo do início dos dias úteis
        data_final   : str, list, np.ndarray, pd.Series
            É a data ou o conjunto de datas que serão utilizadas para cálculo do fim dos dias úteis
        rolagem      : str, None
            Tipo de rolagem executada na data final, caso fornecido None, não performará rolagem
    Reposta:
        du  : np.ndarray
            Resposta do número de dias úteis entre os conjuntos de datas fornecidos
    """
    if rolagem == 'posterior': rolagem = 'forward'
    if rolagem == 'anterior' : rolagem = 'backward'
    
    # Primeiramente criam-se dois np.array para cada uma das variáveis
    data_inicial, data_final = np.array(data_inicial).flatten(), np.array(data_final).flatten()
    #  Caso exista diferença entre o tamanho das variáveis, irá trabalhar com a que tem maior tamanho (v)
    # e repetirá a data da outra variável len(v) vezes
    if data_inicial.shape[0] != data_final.shape[0]:
        if data_inicial.shape[0] == 1:
            data_inicial = np.repeat(data_inicial, data_final.shape[0])
        elif data_final.shape[0] == 1:
            data_final = np.repeat(data_final, data_inicial.shape[0])
        elif data_inicial.shape[0] > 1 and data_final.shape[0] > 1:
          # Caso específico do usuário fornecendo as duas variáveis com mais de 1 elemento mas sem tamanho igual
            raise ValueError(f'O código não aceita data_inicial com tamanho > 1 [{data_inicial.shape[0]} elementos] ao mesmo tempo que data_final tem tamanho > 1 [{data_final.shape[0]} elementos]')

    # As variáveis de datas serão apenas um np.array convertido para o formato de 'datetime64[D]' (formato dos feriados)
    data_inicial, data_final = np.array(np.array([data_inicial, data_final])).astype('datetime64[D]')
    
    if not isinstance(rolagem, type(None)):
        #  Caso queira, o código já rola as datas inicial e final para o dia útil
        # mais próximo
        data_inicial = np.busday_offset(data_inicial, 0, roll = rolagem)
        data_final   = np.busday_offset(data_final, 0, roll = rolagem)
    
    # Cria a matriz_resposta
    matriz_resposta = []
    # Para cada um dos conjuntos de data_inicial - data_final, calculará o range de datas úteis
    for di, df in zip(data_inicial, data_final):
        # Calcula todo o range de datas úteis entre as datas fornecidas
        datas = np.array([workday(di, i) for i in range(int(networkdays(di, df)[0]) + 1)]).flatten()
        # Adiciona a lista de datas para a matriz de resposta
        matriz_resposta.append(datas)
    
    # Matriz de resposta será um np.ndarray flat de todas as listas criadas
    matriz_resposta = np.array(matriz_resposta, dtype = 'object').flatten()
    
    return matriz_resposta
