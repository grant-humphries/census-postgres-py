import os
import sys
import argparse
from os.path import exists, join
from zipfile import ZipFile

import fiona
import sqlalchemy
from sqlalchemy \
    import create_engine, MetaData, Table, Column, Float, Integer, Text
from geoalchemy2 import Geometry
from geoalchemy2.elements import WKTElement
from shapely.geometry import shape

import postgres_acs as pg_acs

TIGER_PRODUCT = {
    'b': 'TABBLOCK10',
    'bg': 'BG',
    't': 'TRACT'
}
TIGER_PRIMARY_KEY = {
    'GEOID',
    'GEOID10'
}


def download_tiger_data():
    """"""

    tiger_url = 'ftp://ftp2.census.gov/geo/tiger/TIGER{yr}'.format(
        yr=ops.tiger_year)

    for pd in ops.product:
        pd_name = TIGER_PRODUCT[pd].lower()
        pd_class = ''.join([c for c in TIGER_PRODUCT[pd] if c.isalpha()])
        pd_dir = join(ops.data_dir, pd_class)

        if not exists(pd_dir):
            os.makedirs(pd_dir)

        for st in ops.states:
            pd_url = '{base_url}/{pd_class}/' \
                          'tl_{yr}_{fips}_{pd_name}.zip'.format(
                               base_url=tiger_url, pd_class=pd_class,
                               yr=ops.tiger_year, fips=states_dict[st],
                               pd_name=pd_name)

            pd_path = pg_acs.download_with_progress(pd_url, pd_dir)
            with ZipFile(pd_path, 'r') as z:
                print '\nunzipping...'
                z.extractall(pd_dir)


def create_tiger_schema(drop_existing=False):
    """"""

    engine = ops.engine
    schema = ops.metadata.schema

    if drop_existing:
        engine.execute("DROP SCHEMA IF EXISTS {} CASCADE;".format(schema))

    try:
        ops.engine.execute("CREATE SCHEMA {};".format(schema))
    except sqlalchemy.exc.ProgrammingError as e:
        print e.message
        print 'Data will be loaded into the existing schema,'
        print 'if you wish recreate the schema use the "drop_existing" flag'


def load_tiger_data():
    """"""

    for pd in ops.product:
        pd_name = TIGER_PRODUCT[pd].lower()
        pd_class = ''.join([c for c in TIGER_PRODUCT[pd] if c.isalpha()])
        pd_dir = join(ops.data_dir, pd_class)

        for st in ops.states:
            shp_name = 'tl_{yr}_{fips}_{pd_name}.shp'.format(
                yr=ops.tiger_year, fips=states_dict[st],
                pd_name=pd_name)
            pd_shp = join(pd_dir, shp_name)

            with fiona.open(pd_shp) as tiger_shape:
                metadata = tiger_shape.meta.copy()
                epsg = int(metadata['crs']['init'].split(':')[1])
                table = create_tiger_table(metadata, pd)

                print '\nloading shapefile "{0}" ' \
                      'into table: "{1}.{2}":'.format(
                           shp_name, ops.metadata.schema, table.name)

                memory_tbl = list()
                max_fid = max(tiger_shape.keys())
                for fid, feat in tiger_shape.items():
                    fields = feat['properties']
                    row = {k.lower(): v for k, v in fields.items()}
                    shapely_geom = shape(feat['geometry'])
                    ga2_geom = WKTElement(shapely_geom.wkt, epsg)
                    row['geom'] = ga2_geom
                    memory_tbl.append(row)

                    if fid % 500 == 0 or fid == max_fid:
                        ops.engine.execute(table.insert(), memory_tbl)
                        memory_tbl = list()

                        if fid % 20000 == 0:
                            sys.stdout.write(str(fid))
                        else:
                            sys.stdout.write('.')


def create_tiger_table(metadata, product, drop_existing=False):
    """metadata parameter must be a fiona metadata object"""

    # handle cases where the table already exists
    table_name = TIGER_PRODUCT[product].lower()
    if not drop_existing:
        engine = ops.engine
        schema = ops.metadata.schema
        if engine.dialect.has_table(engine.connect(), table_name, schema):
            print 'Table {0}.{1} already exists, ' \
                  'using existing table...'.format(schema, table_name)
            print 'to recreate the table use the "drop_existing" flag'
            for table in ops.metadata.tables:
                print table.name
            return ops.metadata.tables[table_name]

    fiona2db = {
        'int': Integer,
        'float': Float,
        'str': Text
    }

    # # it's not possible to make a distinct between polygons and multi-
    # # polygons within shapefiles so we must assume that all polygons
    # # are multi's or postgis may throw an error, fiona classifies all
    # # shape poly's as single polygons and this corrects that
    # geom_type = metadata['schema']['geometry'].upper()
    # if geom_type == 'POLYGON':
    #     geom_type = 'MULTI{}'.format(geom_type)

    columns = list()
    geom_col = Column(
        name='geom',
        type_=Geometry(
            geometry_type='GEOMETRY',
            srid=int(metadata['crs']['init'].split(':')[1])))
    columns.append(geom_col)

    for f_name, f_type in metadata['schema']['properties'].items():
        if f_name in TIGER_PRIMARY_KEY:
            pk_bool = True
        else:
            pk_bool = False

        attr_col = Column(
            name=f_name.lower(),
            type_=fiona2db[f_type.split(':')[0]],
            primary_key=pk_bool)
        columns.append(attr_col)

    table = Table(
        table_name,
        ops.metadata,
        *columns)
    table.create()

    return table


def process_options(arglist=None):
    """Define options that users can pass through the command line, in this
    case these are all postgres database parameters"""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-s', '--states',
        nargs='+',
        required=True,
        choices=sorted(states_dict.keys()),
        help='states for which data is to be include in acs database, '
             'indicate states with two letter postal codes'
    )
    parser.add_argument(
        '-y', '--year',
        required=True,
        dest='tiger_year',
        help='year of the desired TIGER data product'
    )
    parser.add_argument(
        '-dd', '--data_directory',
        default=join(os.getcwd(), 'data', 'TIGER'),
        dest='data_dir',
        help='file path at which downloaded TIGER data is to be saved'
    )
    parser.add_argument(
        '-dp', '--data_product',
        nargs='+',
        default=['b', 'bg', 't'],
        choices=['b', 'bg', 't'],
        dest='product',
        help='desired TIGER data product, choices are: '
             '"b": blocks, "bg": block groups, "t": tracts'
    )
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
        required=True,
        help='postgres password for supplied user'
    )

    options = parser.parse_args(arglist)
    return options


def main():
    """
    >> python postgis_tiger.py -y 2015 -s OR WA -p ur_pass
    """

    global states_dict
    states_dict = pg_acs.get_states_mapping('fips')

    global ops
    args = sys.argv[1:]
    ops = process_options(args)

    pg_conn_str = 'postgres://{user}:{pw}@{host}/{db}'.format(
        user=ops.user, pw=ops.password, host=ops.host, db=ops.dbname)

    ops.engine = create_engine(pg_conn_str)
    ops.metadata = MetaData(
        bind=ops.engine,
        schema='tiger{yr}'.format(yr=ops.tiger_year))

    # download_tiger_data()
    create_tiger_schema(True)
    load_tiger_data()


if __name__ == '__main__':
    main()
