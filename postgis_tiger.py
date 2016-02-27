import os
import sys

import argparse
from glob import glob
from os.path import basename, exists, join, splitext
from zipfile import ZipFile

import fiona
import sqlalchemy
from sqlalchemy import create_engine, MetaData, \
    Table, Column, ForeignKeyConstraint, Float, Integer, Text
from geoalchemy2 import Geometry
from geoalchemy2.elements import WKTElement
from shapely.geometry import shape, MultiPolygon

import utilities as utils
from utilities import GEOHEADER, TIGER_GEOID

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
                               yr=ops.tiger_year, fips=state_fips[st],
                               pd_name=pd_name)

            pd_path = utils.download_with_progress(pd_url, pd_dir)
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
                yr=ops.tiger_year, fips=state_fips[st],
                pd_name=pd_name)
            pd_shp = join(pd_dir, shp_name)

            with fiona.open(pd_shp) as tiger_shape:
                shp_metadata = tiger_shape.meta.copy()
                epsg = int(shp_metadata['crs']['init'].split(':')[1])
                table = create_tiger_table(shp_metadata, pd)

                print '\nloading shapefile "{0}" ' \
                      'into table: "{1}.{2}":'.format(
                           shp_name, ops.metadata.schema, table.name)
                print 'features inserted:'

                memory_tbl = list()
                max_fid = max(tiger_shape.keys())
                for fid, feat in tiger_shape.items():
                    fields = feat['properties']
                    row = {k.lower(): v for k, v in fields.items()}

                    # casting to multipolygon here because a few features
                    # are multi's and the geometry types must match
                    shapely_geom = MultiPolygon([shape(feat['geometry'])])

                    # geoalchemy2 requires that geometry be in EWKT format
                    # for inserts, that conversion is made below
                    ga2_geom = WKTElement(shapely_geom.wkt, epsg)
                    row['geom'] = ga2_geom
                    memory_tbl.append(row)

                    count = fid + 1
                    if count % 1000 == 0 or fid == max_fid:
                        ops.engine.execute(table.insert(), memory_tbl)
                        memory_tbl = list()

                        # logging to inform the user
                        if count % 20000 == 0:
                            sys.stdout.write(str(count))
                        elif fid == max_fid:
                            print '\n'
                        else:
                            sys.stdout.write('..')


def create_tiger_table(shp_metadata, product, drop_existing=False):
    """metadata parameter must be a fiona metadata object"""

    # handle cases where the table already exists
    schema = ops.metadata.schema
    table_name = TIGER_PRODUCT[product].lower()
    if not drop_existing:
        engine = ops.engine
        if engine.dialect.has_table(engine.connect(), table_name, schema):
            full_name = '{0}.{1}'.format(schema, table_name)
            print 'Table {} already exists, ' \
                  'using existing table...'.format(full_name)
            print 'to recreate the table use the "drop_existing" flag'

            return ops.metadata.tables[full_name]

    fiona2db = {
        'int': Integer,
        'float': Float,
        'str': Text
    }

    # it's not possible to make a distinction between polygons and
    # multipolygons within shapefiles, so we must assume geoms of
    # that type are multi's or postgis may throw an error, fiona's
    # metadata always assumes single geoms so multi is appended
    geom_type = shp_metadata['schema']['geometry'].upper()
    if geom_type == 'POLYGON':
        geom_type = 'MULTI{}'.format(geom_type)

    columns = list()
    geom_col = Column(
        name='geom',
        type_=Geometry(
            geometry_type=geom_type,
            srid=int(shp_metadata['crs']['init'].split(':')[1])))
    columns.append(geom_col)

    for f_name, f_type in shp_metadata['schema']['properties'].items():
        col_name = f_name.lower()
        attr_col = Column(
            name=col_name,
            type_=fiona2db[f_type.split(':')[0]])

        if f_name in TIGER_PRIMARY_KEY:
            attr_col.primary_key = True
            pk_col = col_name

        columns.append(attr_col)

    meta_tables = ops.metadata.tables
    geoheaders = [meta_tables[t] for t in meta_tables if GEOHEADER in t]
    for gh in geoheaders:
        foreign_col = gh.columns[TIGER_GEOID]
        fk = ForeignKeyConstraint([pk_col], [foreign_col])
        columns.append(fk)

    table = Table(
        table_name,
        ops.metadata,
        *columns)
    table.create()

    return table


def process_options(arglist=None):
    """Define options that users can pass through the command line, in this
    case these are all postgres database parameters"""

    parser = utils.add_postgres_options(argparse.ArgumentParser())
    parser.add_argument(
        '-s', '--states',
        nargs='+',
        required=True,
        choices=sorted(state_fips.keys()),
        help='states for which data is to be include in acs database, '
             'indicate states with two letter postal codes'
    )
    parser.add_argument(
        '-y', '--year',
        required=True,
        type=int,
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

    options = parser.parse_args(arglist)
    return options


def main():
    """>> python postgis_tiger.py -y 2015 -s OR WA -p ur_pass"""

    global state_fips
    state_fips = utils.get_states_mapping('fips')

    global ops
    args = sys.argv[1:]
    ops = process_options(args)

    pg_url = 'postgres://{user}:{pw}@{host}/{db}'.format(
        user=ops.user, pw=ops.password, host=ops.host, db=ops.dbname)

    ops.engine = create_engine(pg_url)
    ops.metadata = MetaData(
        bind=ops.engine,
        schema='tiger{yr}'.format(yr=ops.tiger_year))

    # the newest acs is generally one year behind the newest tiger data
    # so I'm pairing them as such here
    acs_year = ops.tiger_year - 1
    for i in (1, 3, 5):
        acs_schema = 'acs{yr}_{span}yr'.format(yr=acs_year, span=i)
        try:
            ops.metadata.reflect(schema=acs_schema, only=[GEOHEADER])
        except sqlalchemy.exc.InvalidRequestError:
            pass

    # the newest acs is generally one year behind the newest tiger data
    # so I'm pairing them as such here
    # wild_mod_name = 'acs{yr}_[135]yr'.format(yr=ops.tiger_year - 1)
    # wild_mod_path = join(os.getcwd(), utils.CENSUS_PG_MODEL, wild_mod_name)
    # acs_mod_paths = glob(wild_mod_path)
    #
    # ops.geoheaders = list()
    # if acs_mod_paths:
    #     for mod_path in acs_mod_paths:
    #         mod_name = basename(mod_path)
    #         import_name = '.'.join(
    #             [utils.CENSUS_PG_MODEL, mod_name, utils.GEOHEADER])
    #
    #         module = __import__(import_name, fromlist=[utils.GEOHEADER.title()])
    #         geoheader = getattr(module, utils.GEOHEADER.title())
    #         ops.geoheaders.append(geoheader)

    # download_tiger_data()
    create_tiger_schema(True)
    load_tiger_data()
    utils.generate_model(ops.metadata)


if __name__ == '__main__':
    main()
