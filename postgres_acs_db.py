import os
import csv
import sys
import zipfile
import urllib2
import argparse
from pprint import pprint
from os.path import dirname, exists, join

import xlrd
from sqlalchemy \
    import create_engine, Column, Table, Integer, Float, String, MetaData
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

    # get raw census data in text delimited form, the data has been
    # grouped into what the Census Bureau calls 'sequences'
    acs_url = 'http://www2.census.gov/programs-surveys/' \
              'acs/summary_file/{yr}'.format(yr=ops.acs_year)

    for geog in GEOGRAPHY:
        geog_dir = join(ops.data_dir, geog.lower())

        if not exists(geog_dir):
            os.makedirs(geog_dir)

        for st in ops.states:
            st_name = STATE_DICT[st]
            geog_url = '{base_url}/data/{span}_year_by_state/' \
                       '{state}_{geography}.zip'.format(
                            base_url=acs_url, span=ops.span,
                            state=st_name, geography=geog)

            geog_path = download_with_progress(geog_url, geog_dir)
            with zipfile.ZipFile(geog_path, 'r') as z:
                print 'unzipping...'
                z.extractall(dirname(geog_path))

    # the raw csv doesn't have field names for metadata, the templates
    # downloaded below provide that (but only the geoheader metadata
    # will be used by this process)
    schema_url = '{base_url}/data/{yr}_{span}yr_' \
                 'Summary_FileTemplates.zip'.format(
                      base_url=acs_url, yr=ops.acs_year, span=ops.span)

    schema_path = download_with_progress(schema_url, ops.data_dir)
    with zipfile.ZipFile(schema_path, 'r') as z:
        print 'unzipping...'
        z.extractall(dirname(schema_path))

    # download the lookup table that contains information as to how to
    # extract the ACS tables from the sequences
    lookup_url = '{base_url}/documentation/user_tools/' \
                 '{lookup}'.format(base_url=acs_url, lookup=ops.lookup_file)
    download_with_progress(lookup_url, ops.data_dir)


def download_with_progress(url, dir):
    """"""

    # function adapted from: http://stackoverflow.com/questions/22676

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
        status += chr(8) * (len(status) + 1)
        print status,

    f.close()

    return file_path


def create_database_and_schema():
    """"""

    engine = ops.engine
    if not database_exists(engine.url):
        create_database(engine.url)

    engine.execute("DROP SCHEMA IF EXISTS {} CASCADE;".format(
        ops.metadata.schema))
    engine.execute("CREATE SCHEMA {};".format(ops.metadata.schema))


def create_geoheader():
    """"""

    geo_xls = '{yr}_SFGeoFileTemplate.xls'.format(yr=ops.acs_year)
    geo_schema = join(ops.data_dir, geo_xls)
    book = xlrd.open_workbook(geo_schema)
    sheet = book.sheet_by_index(0)

    meta_fields = []
    blank_counter = 1
    for cx in xrange(sheet.ncols):
        field = {
            'id': sheet.cell_value(0, cx).lower(),
            'comment': sheet.cell_value(1, cx),
            'type': String
        }
        if field['id'].upper() in PRIMARY_KEY:
            field['pk'] = True
        else:
            field['pk'] = False

        # there are multiple fields called 'blank' that are reserved
        # for future use, but columns in the same table cannot have
        # the same name
        if field['id'] == 'blank':
            field['id'] += str(blank_counter)
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
                table = Table(
                    'geoheader', ops.metadata,
                    *(Column(
                        f['id'], f['type'],
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

    acs_tables = dict()
    lookup_path = join(ops.data_dir, ops.lookup_file)
    with open(lookup_path) as lookup:
        reader = csv.DictReader(lookup)
        for row in reader:
            if row['Start Position'].isdigit():
                meta_table = {
                    'id': row['Table ID'].lower(),
                    'sequence': row['Sequence Number'],
                    'start_ix': int(row['Start Position']) - 1,
                    'cells': int(''.join(
                        [i for i in row['Total Cells in Table']
                         if i.isdigit()])),
                    'comment': row['Table Title'],
                    'num_type': Integer,
                    'fields': [
                        {
                            'id': 'stusab',
                            'comment': 'State Postal Abbreviation',
                            'type': String,
                            'pk': True
                        },
                        {
                            'id': 'logrecno',
                            'comment': 'Logical Record Number',
                            'type': String,
                            'pk': True
                        }
                    ]
                }
                acs_tables[row['Table ID']] = meta_table

            # the universe of the table subject matter is stored in a
            # separate row, add it to the table comment
            elif not row['Line Number'].strip() \
                    and not row['Start Position'].strip():
                cur_table = acs_tables[row['Table ID']]
                cur_table['comment'] += ', {}'.format(row['Table Title'])

            # from what I can ascertain of row number of 0.5 indicates
            # that a tables number values are float instead of the
            # default integer
            elif row['Line Number'] == '0.5':
                acs_tables[row['Table ID']]['num_type'] = Float

            # note that there are some rows with a line number of '0.5'
            # I'm not totally clear on what purpose they serve, but they
            # are not row in the tables and are being excluded here.
            elif row['Line Number'].isdigit():
                cur_table = acs_tables[row['Table ID']]
                meta_field = {
                    'id': '_' + row['Line Number'],
                    'comment': row['Table Title'],
                    'type': cur_table['num_type'],
                    'pk': False
                }
                cur_table['fields'].append(meta_field)

    stusab_ix = 2
    logrec_ix = 5
    for mt in acs_tables.values():
        table = Table(mt['id'], ops.metadata,
                      *(Column(f['id'], f['type'],
                               primary_key=f['pk'],
                               doc=f['comment'])
                        for f in mt['fields'])
                      )
        table.create()

        # create a list of the indices that for the columns that will
        # be extracted from the defined sequence for the current table
        tbl_cols = [stusab_ix, logrec_ix]
        tbl_cols.extend(
            xrange(mt['start_ix'], mt['start_ix'] + mt['cells'])
        )

        for st in ops.states:
            seq_name = 'e{yr}{span}{state}{seq}000.txt'.format(
                yr=ops.acs_year, span=ops.span,
                state=st.lower(), seq=mt['sequence']
            )
            for geog in GEOGRAPHY:
                seq_path = join(ops.data_dir, geog, seq_name)
                with open(seq_path) as seq:
                    reader = csv.reader(seq)
                    for row in reader:
                        tbl_vals = list()
                        for i in row:
                            if i in tbl_cols:
                                # data clean up
                                if i == stusab_ix:
                                    row[i] = row[i].upper()
                                elif row[i] == '.':
                                    row[i] = 0.0
                                elif row[i] == '':
                                    row[i] = None

                                tbl_vals.append(row[i])
                                print tbl_vals
                                exit()
                        table.insert(tbl_vals).execute()


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

    pg_conn_str = 'postgres://{user}:{pw}@{host}/{db}'.format(
        user=ops.user, pw=ops.password, host=ops.host, db=ops.dbname)

    ops.lookup_file = 'ACS_{span}yr_Seq_Table_Number_' \
                      'Lookup.txt'.format(span=ops.span)
    ops.engine = create_engine(pg_conn_str)
    ops.metadata = MetaData(
        bind=ops.engine,
        schema='acs{yr}_{span}yr'.format(yr=ops.acs_year,
                                         span=ops.span)
    )
    # download_acs_data()
    create_database_and_schema()
    create_geoheader()
    create_acs_tables()


if __name__ == '__main__':
    main()
