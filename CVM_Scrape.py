import requests
import os
from zipfile import ZipFile
import pandas as pd
from tqdm import tqdm

def Scrape(diretorio):
    """
    Webscrape do site da CVM para obter os arquivos zip, e arquivos csv contido neles, disponibilizados no site da CVM

    :param diretorio: Diretório para onde vão os arquivos baixados.
    :type diretorio: str
    """    
    for tipo in ['BPA','BPP','DFC_MD','DFC_MI','DMPL','DRE','DVA','FRE', 'FCA', 'ITR','DFP', 'IPE']:
        print(f'Iniciando download dos arquivos do tipo {tipo}')
        for i in range(10,21):
            
            #Baixando o arquivo zip da página
            req = requests.get('http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{}/DADOS/{}_cia_aberta_20{}.zip'.format(str(tipo), str(tipo).lower(),str(i)), stream=True)
            
            #Caso a página retornar o erro de "não encontrado"
            if req.status_code == 404:
                print(f'\tArquivo de 20{str(i)} não encontrado')
                continue
            
            #Cria diretório para baixar os arquivos zip
            if not os.path.exists(diretorio+'\CVM_scrape'):
                os.mkdir(diretorio+'\CVM_scrape')
            
            #Salva os arquivos zip no diretório criado
            with open('{}\\CVM_scrape\\{}_arquivo_20{}.zip'.format(str(diretorio),str(tipo),str(i)), 'wb') as file:
                for chunk in req.iter_content(chunk_size=128):
                    file.write(chunk)

            #Extrai os dados dos arquivos zip, separando em pastas de acordo com a divisão de dados feita pela CVM
            ZipFile('{}\\CVM_scrape\\{}_arquivo_20{}.zip'.format(str(diretorio),str(tipo),str(i)), 'r').extractall(f'CVM_{tipo}')
            
            print(f'\t20{str(i)} concluído')
        print(f'Arquivos do tipo {tipo} concluídos.\n')
    
    print('__________LIMPANDO__________')
    
    #Iterando pela divisão de dados 
    for dir in ['FRE', 'FCA', 'ITR','DFP', 'IPE']:
        
        #Diretório completo para a pasta do atual segmento de dados
        dir = diretorio+'\CVM_'+dir
        
        #Lista para as divisões de dados dentro do atual segmento
        archives = []

        #Iterando pelas divisões
        for archive in os.listdir(dir):

            #Removendo o ano do nome do arquivo e adicionando a lista
            archive = archive[:-7:]
            archives.append(archive)

        #Agora temos os diferentes arquivos que queremos em uma lista, sem segmentação de ano
        archives = list(set(archives))

        #Iterando pelos diferentes arquivos
        for type in archives:
            print(dir+'\\'+type)
            consolidated = pd.DataFrame()
            for csv in os.listdir(dir):
                if type in csv:
                    consolidated = pd.concat([consolidated, pd.read_csv(dir+'\\'+csv, sep=';', decimal=',',error_bad_lines=False, encoding='ISO 8859-1')] )
                    os.remove(dir+'\\'+csv)
            consolidated.to_csv(dir+'\\'+type[:-1]+'_historical.csv', index=False)


if __name__ == "__main__":

    Scrape('D:\Data\SP_Bovespa')
        


