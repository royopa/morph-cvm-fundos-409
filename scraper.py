# -*- coding: utf-8 -*-
import requests
from tqdm import tqdm
import os
from zipfile import ZipFile
import pandas as pd
import scraperwiki


def download_file(url, file_name):
    response = requests.get(url, stream=True)
    with open(file_name, "wb") as handle:
        for data in tqdm(response.iter_content()):
            handle.write(data)
    handle.close()

    
def create_download_folder():
    # Create directory
    dirName = os.path.join('downloads')
 
    try:
        # Create target Directory
        os.mkdir(dirName)
        print("Directory", dirName, "Created ")
    except FileExistsError:
        print("Directory", dirName, "already exists")
    
    
def extract_file(path_file):
    with ZipFile(path_file, 'r') as zipObj:
       # Get a list of all archived file names from the zip
       list_of_file_names = zipObj.namelist()
       # Iterate over the file names
       for file_name in list_of_file_names:
        if file_name.endswith('.txt'):
            print('Extracting', file_name)
            zipObj.extract(file_name)


def import_txt_file(path_extracted_file):
    df = pd.read_csv(
        path_extracted_file,
        encoding='iso-8859-1',
        sep='\t'
    )

    print('Importing {} items'.format(len(df)))

    for index, row in df.iterrows():
        data = {
            'CNPJ': row['CNPJ'],
            'DT_REG': row['DT_REG'],
            'CD_CVM': row['CD_CVM'],
            'VL_PATRIM_LIQ': row['VL_PATRIM_LIQ'],
            'DENOM_SOCIAL': row['DENOM_SOCIAL'],
            'CLASSE': row['CLASSE'],
            'CONDOM': row['CONDOM'],
            'FUNDO_COTAS': row['FUNDO_COTAS'],
            'RENTAB': row['RENTAB'],
            'TRIB_LPRAZO': row['TRIB_LPRAZO'],
            'INVEST_QUALIF': row['INVEST_QUALIF'],
            'EXCLUSIVO': row['EXCLUSIVO'],
            'ADMIN': row['ADMIN'],
            'CNPJ_ADMIN': row['CNPJ_ADMIN'],
            'SIT': row['SIT'],
            'DT_INI_SIT': row['DT_INI_SIT'],
            'CNPJ_GESTOR': row['CNPJ_GESTOR'],
            'GESTOR': row['GESTOR'],
            'DT_INI_ATIV': row['DT_INI_ATIV'],
            'DT_CONST': row['DT_CONST']
        }

        scraperwiki.sqlite.save(unique_keys=['CNPJ', 'CD_CVM'], data=data)


def process_import(file_name_server, file_name_extracted):
    print(
        "**************************************************",
        "Starting process", 
        "file name server: '{}'".format(file_name_server),
        "extracted file: '{}'".format(file_name_extracted),
        sep = '\n',
        end = '\n\n'
    )

    # format the variables names do use in whole process    
    url = 'http://sistemas.cvm.gov.br/cadastro/{}'.format(file_name_server)
    path_zip_file = os.path.join('downloads', file_name_server)
    path_extracted_file = os.path.join(file_name_extracted)
    
    # download and extract file
    download_file(url, path_zip_file)
    extract_file(path_zip_file)
    
    # import extracted file to sqlite database
    import_txt_file(path_extracted_file)
    
    # remove files alread used
    os.remove(path_zip_file)
    os.remove(path_extracted_file)


def main():
    # format the name of database used for morph.io
    os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'
    
    # create download folder
    create_download_folder()

    # fundos 409 ativos
    file_name_server = 'SPW_FI.ZIP'
    file_name_extracted = 'SPW_FI.txt'
    process_import(file_name_server, file_name_extracted)

    # fundos 409 cancelados
    file_name_server = 'SPW_FI_CANCELADOS.ZIP'
    file_name_extracted = 'SPW_FI_CANCELADOS.txt'
    process_import(file_name_server, file_name_extracted)


if __name__ == '__main__':
    main()