import csv
from pathlib import Path
from datetime import datetime

from kbc.env_handler import KBCEnvHandler
import sys
import os
import logging
from kbc.csv_tools import CachedOrthogonalDictWriter

# configuration variables
KEY_PRINT_ROWS = 'print_rows'
KEY_DEBUG = 'debug'

MANDATORY_PARS = []
MANDATORY_IMAGE_PARS = []

APP_VERSION = '0.0.1'


class Component(KBCEnvHandler):
    def __init__(self, debug=False):
        # for easier local project setup
        default_data_dir = Path(__file__).resolve().parent.parent.joinpath('data').as_posix() \
            if not os.environ.get('KBC_DATADIR') else None

        KBCEnvHandler.__init__(self, MANDATORY_PARS, log_level=logging.DEBUG if debug else logging.INFO,
                               data_path=default_data_dir)

        # override debug from config
        if self.cfg_params.get(KEY_DEBUG):
            debug = True
        if debug:
            logging.getLogger().setLevel(logging.DEBUG)
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validate_config(MANDATORY_PARS)
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as e:
            logging.exception(e)
            exit(1)

    def run(self):
        '''
        Main execution code
        '''
        params = self.cfg_params  # noqa

        print('Running...')
        state = self.get_state_file()
        print('Last update: %s' % state.get('last_update'))

        with open(self.get_input_tables_definitions()[0].full_path, 'r') as input:
            reader = csv.DictReader(input)
            new_columns = reader.fieldnames
            # append row number col
            new_columns.append('row_number')

            outputFilename = '%s/%s' % (self.tables_out_path, self.get_input_tables_definitions()[0].file_name)
            self.configuration.write_table_manifest( 
                outputFilename,
                primary_key=['row_number'],
                incremental=True,
                columns=new_columns 
            )

            with CachedOrthogonalDictWriter(outputFilename, new_columns) as writer:
                for index, l in enumerate(reader):
                    # print line
                    if params.get(KEY_PRINT_ROWS):
                        print(f'Printing line {index}: {l}')
                    # add row number
                    l['row_number'] = index
                    writer.writerow(l)

        state['last_update'] = datetime.utcnow().timestamp()
        # state = {'last_update': datetime.utcnow().timestamp()}

        self.write_state_file(state)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug_arg = sys.argv[1]
    else:
        debug_arg = False
    try:
        comp = Component(debug_arg)
        comp.run()
    except Exception as exc:
        logging.exception(exc)
        exit(1)
