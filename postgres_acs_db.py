import os
import csv
import sys
import zipfile
import urllib2
import argparse
from pprint import pprint
from os.path import dirname, exists, join
from collections import defaultdict

import xlrd
from sqlalchemy import create_engine, Column, Table, Integer, String, MetaData
from sqlalchemy_utils import database_exists, create_database

# geography groupings offered by the Census Bureau
GEOGRAPHY = [
    'Tracts_Block_Groups_Only',
    'All_Geographies_Not_Tracts_Block_Groups'
]
PRIMARY_KEY = [
    'STUSAB',
    'LOGRECNO'
]
STATE_DICT = {
    'OR': 'Oregon',
    'WA': 'Washington'
}

def download_acs_data():
    """"""

    'ACS_{span}yr_Seq_Table_Number_Lookup.txt'

    acs_url = 'http://www2.census.gov/programs-surveys/' \
              'acs/summary_file/{yr}/data'.format(yr=ops.acs_year)

    for geog in GEOGRAPHY:
        geog_dir = join(ops.data_dir, geog.lower())

        if not exists(geog_dir):
            os.makedirs(geog_dir)

        for st in ops.states:
            st_name = STATE_DICT[st]
            geog_url = '{base_url}/{span}_year_by_state/' \
                       '{state}_{geography}.zip'.format(
                           base_url=acs_url, span=ops.span,
                           state=st_name, geography=geog)

            geog_path = download_with_progress(geog_url, geog_dir)
            with zipfile.ZipFile(geog_path, 'r') as z:
                z.extractall(dirname(geog_path))

    schema_url = '{base_url}/{yr}_{span}yr_' \
                 'Summary_FileTemplates.zip'.format(
                     base_url=acs_url, yr=ops.acs_year, span=ops.span)

    schema_path = download_with_progress(schema_url, ops.data_dir)
    with zipfile.ZipFile(schema_path, 'r') as z:
        z.extractall(dirname(schema_path))


def download_with_progress(url, dir):
    """"""

    # code adapted from: http://stackoverflow.com/questions/22676

    file_name = url.split('/')[-1]
    file_path = join(dir, file_name)
    u = urllib2.urlopen(url)
    f = open(file_path, 'wb')
    meta = u.info()
    file_size = int(meta.getheaders("Content-Length")[0])
    print "Downloading: %s Bytes: %s" % (file_name, file_size)

    file_size_dl = 0
    block_sz = 8192
    while True:
        buffer_ = u.read(block_sz)
        if not buffer_:
            break

        file_size_dl += len(buffer_)
        f.write(buffer_)

        status = '{0:10d}  [{2:3.2f}%]'.format(
            file_size_dl, file_size, file_size_dl * 100. / file_size)
        status += chr(8) * (len(status)+1)
        print status,

    f.close()

    return file_path


def create_database_and_schema():
    """"""

    pg_conn_str = 'postgres://{user}:{pw}@{host}/{db}'.format(
        user=ops.user, pw=ops.password, host=ops.host, db=ops.dbname)

    engine = create_engine(pg_conn_str)
    if not database_exists(engine.url):
        create_database(engine.url)

    engine.execute("DROP SCHEMA IF EXISTS {} CASCADE;".format(ops.schema))
    engine.execute("CREATE SCHEMA {};".format(ops.schema))

    return engine


def create_geoheader():
    """"""

    engine = create_database_and_schema()
    metadata = MetaData(bind=engine, schema=ops.schema)

    geo_xls = '{yr}_SFGeoFileTemplate.xls'.format(yr=ops.acs_year)
    geo_schema = join(ops.data_dir, geo_xls)
    book = xlrd.open_workbook(geo_schema)
    sheet = book.sheet_by_index(0)

    meta_fields = []
    blank_counter = 1
    for cx in xrange(sheet.ncols):
        field = {
            'name': sheet.cell_value(0, cx).lower(),
            'comment': sheet.cell_value(1, cx)
        }
        if field['name'].upper() in PRIMARY_KEY:
            field['pk'] = True
        else:
            field['pk'] = False

        # there are multiple fields called 'blank' that are reserved
        # for future use, but columns in the same table cannot have
        # the same name
        if field['name'] == 'blank':
            field['name'] += str(blank_counter)
            blank_counter += 1

        meta_fields.append(field)

    table = None
    geog_dir = join(ops.data_dir, GEOGRAPHY[0].lower())
    for st in ops.states:
        geo_csv = 'g{yr}{span}{state}.csv'.format(
            yr=ops.acs_year, span=ops.span, state=st.lower()
        )
        with open(join(geog_dir, geo_csv)) as geo_data:
            reader = csv.reader(geo_data)

            if table is None:
                test_row = next(reader)
                geo_data.seek(0)

                for field, ex in zip(meta_fields, test_row):
                    if ex.isdigit():
                        field['type'] = Integer
                    else:
                        field['type'] = String

                table = Table(
                    'geoheader', metadata,
                    *(Column(
                        f['name'],
                        String,
                        primary_key=f['pk'],
                        doc=f['comment'])
                      for f in meta_fields))
                table.create()

            for row in reader:
                # null values come in from the csv as empty strings
                # this converts them such that they will be NULL in
                # the database
                null_row = [v if v == 0 else v or None for v in row]
                table.insert(null_row).execute()


def create_acs_tables():
    """"""

    base_ix = 6
    meta_tables = {}
    seq_schema_dir = join(ops.data_dir, 'seq')

    for i, seq in enumerate(os.listdir(seq_schema_dir)):
        seq_path = join(seq_schema_dir, seq)
        book = xlrd.open_workbook(seq_path)
        sheet = book.sheet_by_name('E')

        if i == 0:
            base_cols = []
            for cx in xrange(base_ix):
                meta_field = {
                    'name': sheet.cell_value(0, cx),
                    'comment': None
                }
                base_cols.append(meta_field)

        # create copy of base_cols so original is not modified
        fields = list(base_cols)
        for cx in xrange(base_ix, sheet.ncols):
            table, col = sheet.cell_value(0, cx).split('_')
            comment = sheet.cell_value(1, cx)
            col_name = '_{}'.format(int(col))

            meta_field = {
                'name': col_name,
                'comment': comment
            }
            fields.append(meta_field)
            meta_tables[table] = fields

    for


def detect_csv_data_types():
    """"""

    pass


def process_options(arg_list=None):
    """"""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-s', '--states',
        nargs='+',
        required=True,
        dest='states',
        help='states for which is to be include in acs database, '
             'indicate states with two letter postal codes'
    )
    parser.add_argument(
        '-y', '--year',
        required=True,
        dest='acs_year',
        help='most recent year covered by acs data product'
    )
    parser.add_argument(
        '-l', '--length', '--span',
        default=5,
        choices=(1, 3, 5),
        dest='span',
        help='number of years that acs data product covers'
    )
    parser.add_argument(
        '-dd', '--data_directory',
        default=join(os.getcwd(), 'data'),
        dest='data_dir',
        help='file path at which downloaded census data is to be saved'
    )
    parser.add_argument(
        '-H', '--host',
        default='localhost',
        dest='host',
        help='postgres database server host'
    )
    parser.add_argument(
        '-u', '--user',
        default='postgres',
        dest='user',
        help='postgres database user name'
    )
    parser.add_argument(
        '-d', '--dbname',
        default='census',
        dest='dbname',
        help='database name to create/connect to'
    )
    parser.add_argument(
        '-p', '--password',
        dest='password',
        help='postgres data base password for supplied user'
    )

    options = parser.parse_args(arg_list)
    return options


def main():
    """"""

    global ops
    args = sys.argv[1:]
    ops = process_options(args)
    ops.schema = 'acs{yr}_{span}yr'.format(
        yr=ops.acs_year, span=ops.span
    )

    # download_acs_data()
    # create_database_and_schema()
    # create_geoheader()
    create_acs_tables()

if __name__ == '__main__':
    main()
