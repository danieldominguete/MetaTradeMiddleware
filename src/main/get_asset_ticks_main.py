'''
===========================================================================================
Get Asset Ticks Main : Get ticks infos from Metatrade5 (just windows environment)
===========================================================================================
'''

# =========================================================================================
# Importing the libraries
import logging
import os, sys, inspect
import argparse
from datetime import datetime
import pandas as pd
import MetaTrader5 as mt5
import pytz

# Include root folder to path
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(os.path.dirname(currentdir))
sys.path.insert(0,parentdir)

# Local libraries
from src.lib.util import Util

class GetAssetTicksMain:

    def __init__(self, parameters_file):

        '''Constructor for this class'''
        self.parameters_file = parameters_file

    def run(self):

        # =========================================================================================
        # Script Setup
        logging.info("======================================================================")
        logging.info("Iniciando script de aquisição de série de candles...")

        # Parameters
        asset_name = "VVAR3"
        candlestick_interval = mt5.TIMEFRAME_M1

        # =========================================================================================
        # Metatrader connection
        logging.info("======================================================================")
        logging.info("Conectando com o Metatrade5 em execução na máquina local:")

        # MT5 init
        if not mt5.initialize():
            print("Falha na inicialização do connector com o MT5. Código de erro: " + mt5.last_error())
            mt5.shutdown()
            quit()

        # MT5 Infos
        print("Versão do Metatrader: " + str(mt5.version()))

        terminal_info = mt5.terminal_info()
        if terminal_info is not None:
            print("Informações detalhadas da conexão:")
            terminal_info_dict = terminal_info._asdict()
            for prop in terminal_info_dict:
                print("  {}={}".format(prop, terminal_info_dict[prop]))

        # definimos o fuso horário como UTC
        timezone = pytz.timezone("Etc/UTC")

        # criamos o objeto datetime no fuso horário UTC para que não seja aplicado o deslocamento do fuso horário local
        utc_from = datetime(2020, 7, 10, hour=00, tzinfo=timezone)
        utc_to = datetime(2020, 7, 11, hour=00, tzinfo=timezone)

        # obtemos as barras no fuso horário UTC
        candlesticks = mt5.copy_rates_range(asset_name, candlestick_interval , utc_from, utc_to)

        # criamos a partir dos dados obtidos DataFrame
        rates_frame = pd.DataFrame(candlesticks)

        # convertemos o tempo em segundos no formato datetime
        rates_frame['time'] = pd.to_datetime(rates_frame['time'], unit='s')

        # Closing connection
        mt5.shutdown()

        # Saving to file
        Util.save_pandas_file(dataframe=rates_frame, file_path="asset_dump_example.csv")

        logging.info("======================================================================")
        logging.info("Script de aquisição de dados encerrado!")
        logging.info("======================================================================")

# ===========================================================================================
# Main call from terminal
if __name__ == "__main__":
    '''
    Call from terminal command
    '''

    # getting script arguments
    parser = argparse.ArgumentParser(
        description='Metatrade Middleware - Script Main for Data Aquisition'
    )
    parser.add_argument('-f', '--config_file_json',
                        help='Json config file for script execution', required=True)

    args = parser.parse_args()

    # Running script main
    try:
        processor = GetAssetTicksMain(parameters_file=args.config_file_json)
        processor.run()
    except:
        logging.error('Ops ' + str(sys.exc_info()[0]) + ' occured!')
        raise
# ===========================================================================================
# ===========================================================================================
