# Utilities that are used by multiple scripts in the census-postgres-py
# project

import os
import csv
import sys
import subprocess
import urllib2
from os.path import dirname, exists, join

ACS_SPANS = (1, 3, 5)
GEOHEADER = 'geoheader'
MODEL = 'model'
TIGER_GEOID = 'tiger_geoid'


def get_states_mapping(value_type):
    """Maps state abbreviations to their full name or FIPS code"""

    value_dict = {'name': 'State', 'fips': 'FIPS Code'}
    try:
        value_field = value_dict[value_type]
    except KeyError:
        print 'Invalid value type supplied for states mapping'
        print 'options are: "name" and "fips"'
        exit()

    states = dict()
    states_csv_path = join(dirname(sys.argv[0]), 'census_states.csv')
    with open(states_csv_path) as states_csv:
        reader = csv.DictReader(states_csv)
        for r in reader:
            states[r['Abbreviation']] = r[value_field].replace(' ', '_')

    return states


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


def generate_model(metadata, table_groups=None):
    """"""

    url = metadata.bind.url
    schema = metadata.schema
    model_dir = join(os.getcwd(), MODEL)

    if not table_groups:
        table_groups = dict()
        for schema_table in metadata.tables:
            table = schema_table.split('.')[1]
            table_groups[table] = [table]

    if not exists(model_dir):
        os.makedirs(model_dir)
        open(join(model_dir, '__init__.py'), 'w').close()

    schema_dir = join(model_dir, schema)
    if not exists(schema_dir):
        os.makedirs(schema_dir)
        open(join(schema_dir, '__init__.py'), 'w').close()

    codegen_template = './bin/sqlacodegen ' \
                       '--schema {0} --tables {1} ' \
                       '--outfile {2} {url}'

    print '\ngenerating sqlalchemy model at: {}'.format(schema_dir)
    print 'table groups written:'

    i = 0
    for table_key, table_list in table_groups.items():
        table_str = ','.join(sorted(table_list))
        model_file = join(schema_dir, '{}.py'.format(table_key))
        codegen = codegen_template.format(
            schema, table_str, model_file, url=url)
        subprocess.call(codegen)

        # logging for user
        i += 1
        if i % 50 == 0:
            sys.stdout.write(str(i))
        else:
            sys.stdout.write('.')


def add_postgres_options(parser):
    """"""

    # if the PGPASSWORD environment variable has been set use it
    password = os.environ.get('PGPASSWORD')
    if password:
        pw_require = False
    else:
        pw_require = True

    parser.add_argument(
        '-H', '--host',
        default='localhost',
        help='url of postgres host server'
    )
    parser.add_argument(
        '-u', '--user',
        default='postgres',
        help='postgres user name'
    )
    parser.add_argument(
        '-d', '--dbname',
        default='census',
        help='name of target database'
    )
    parser.add_argument(
        '-p', '--password',
        required=pw_require,
        default=password,
        help='postgres password for supplied user'
    )

    return parser
