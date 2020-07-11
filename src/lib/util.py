'''
===========================================================================================
Utils Package
===========================================================================================
Script Reviewed by COGNAS
===========================================================================================
'''
import logging, sys
import operator
from functools import reduce
import json
import pandas as pd

class Util:

    def __init__(self):
        '''Constructor for this class'''

    @staticmethod
    def load_parameters_from_file(path_file):

        try:
            with open(path_file) as json_file:
                data = json.load(json_file)
        except:
            logging.error('Ops ' + str(sys.exc_info()[0]) + ' occured!')
            raise

        return data

    @staticmethod
    def save_pandas_file(dataframe:pd, file_path:str):
        dataframe.to_csv(file_path,
                         sep='\t',
                         index=True,
                         index_label="id",
                         encoding='utf-8')
