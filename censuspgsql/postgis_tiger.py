import os
import sys
from argparse import ArgumentParser
from functools import partial
from os.path import exists, join
from zipfile import ZipFile

import fiona
import pyproj
import sqlalchemy
from geoalchemy2 import Geometry
from geoalchemy2.elements import WKTElement
from shapely import ops
from shapely.geometry import shape, MultiPolygon
from sqlalchemy import create_engine, MetaData, \
    Table, Column, ForeignKeyConstraint, Float, Integer, Text

import censuspgsql.utilities as utils
from censuspgsql.utilities import ACS_SPANS, GEOHEADER, \
    GEOID, TIGER_GEOID, TIGER_MOD

TIGER_PK = GEOID
TIGER_PRODUCT = {
    'b': 'TABBLOCK10',
    'bg': 'BG',
    't': 'TRACT'
}


def download_tiger_data():
    """"""

    tiger_url = 'ftp://ftp2.census.gov/geo/tiger/TIGER{yr}'.format(
        yr=go.tiger_year)

    for pd in go.product:
        pd_name = TIGER_PRODUCT[pd].lower()
        pd_class = ''.join([c for c in TIGER_PRODUCT[pd] if c.isalpha()])
        pd_dir = join(go.data_dir, pd_class)

        if not exists(pd_dir):
            os.makedirs(pd_dir)

        for st in go.states:
            pd_url = '{base_url}/{pd_class}/' \
                      'tl_{yr}_{fips}_{pd_name}.zip'.format(
                           base_url=tiger_url, pd_class=pd_class,
                           yr=go.tiger_year, fips=go.state_fips[st],
                           pd_name=pd_name)

            pd_path = utils.download_with_progress(pd_url, pd_dir)
            with ZipFile(pd_path, 'r') as z:
                print '\nunzipping...'
                z.extractall(pd_dir)


def create_tiger_schema(drop_existing=False):
    """"""

    engine = go.engine
    schema = go.metadata.schema

    if drop_existing:
        engine.execute("DROP SCHEMA IF EXISTS {} CASCADE;".format(schema))

    try:
        go.engine.execute("CREATE SCHEMA {};".format(schema))
    except sqlalchemy.exc.ProgrammingError as e:
        print e.message
        print 'Data will be loaded into the existing schema,'
        print 'if you wish recreate the schema use the "drop_existing" flag'


def load_tiger_data():
    """"""

    if go.epsg:
        transformation = partial(
            pyproj.transform,
            pyproj.Proj(init='epsg:{}'.format(src_srs)),
            pyproj.Proj(init='epsg:{}'.format(go.epsg), preserve_units=True)
        )


    # if the foreign key flag is set to true reflect geoheader tables
    # in matching acs schemas, tiger data is matched to acs that is one
    # year less recent because it is released one year sooner
    if go.foreign_key:
        acs_year = go.tiger_year - 1
        for i in ACS_SPANS:
            acs_schema = 'acs{yr}_{span}yr'.format(yr=acs_year, span=i)
            try:
                go.metadata.reflect(schema=acs_schema, only=[GEOHEADER])
            except sqlalchemy.exc.InvalidRequestError:
                pass

    for pd in go.product:
        pd_name = TIGER_PRODUCT[pd].lower()
        pd_class = ''.join([c for c in TIGER_PRODUCT[pd] if c.isalpha()])
        pd_dir = join(go.data_dir, pd_class)

        for st in go.states:
            shp_name = 'tl_{yr}_{fips}_{pd_name}.shp'.format(
                yr=go.tiger_year, fips=go.state_fips[st],
                pd_name=pd_name)
            pd_shp = join(pd_dir, shp_name)

            with fiona.open(pd_shp) as tiger_shape:
                shp_metadata = tiger_shape.meta.copy()
                epsg = int(shp_metadata['crs']['init'].split(':')[1])
                table = create_tiger_table(shp_metadata, pd)

                print '\nloading shapefile "{0}" ' \
                      'into table: "{1}.{2}":'.format(
                           shp_name, go.metadata.schema, table.name)
                print 'features inserted:'

                memory_tbl = list()
                max_fid = max(tiger_shape.keys())
                for fid, feat in tiger_shape.items():
                    fields = feat['properties']
                    row = {k.lower(): v for k, v in fields.items()}

                    # casting to multipolygon here because a few features
                    # are multi's and the geometry types must match
                    shapely_geom = MultiPolygon([shape(feat['geometry'])])

                    if go.transform:
                        shapely_geom = ops.transform(transformation, shapely_geom)

                    # geoalchemy2 requires that geometry be in EWKT format
                    # for inserts, that conversion is made below
                    ga2_geom = WKTElement(shapely_geom.wkt, epsg)
                    row['geom'] = ga2_geom
                    memory_tbl.append(row)

                    count = fid + 1
                    if count % 1000 == 0 or fid == max_fid:
                        go.engine.execute(table.insert(), memory_tbl)
                        memory_tbl = list()

                        # logging to inform the user
                        if count % 20000 == 0:
                            sys.stdout.write(str(count))
                        elif fid == max_fid:
                            print '\n'
                        else:
                            sys.stdout.write('..')


def create_tiger_table(shp_metadata, product, drop_existing=False):
    """shp_metadata parameter must be a fiona metadata object"""

    # handle cases where the table already exists
    schema = go.metadata.schema
    table_name = TIGER_PRODUCT[product].lower()
    if not drop_existing:
        engine = go.engine
        if engine.dialect.has_table(engine.connect(), table_name, schema):
            full_name = '{0}.{1}'.format(schema, table_name)
            print 'Table {} already exists, ' \
                  'using existing table...'.format(full_name)
            print 'to recreate the table use the "drop_existing" flag'

            return go.metadata.tables[full_name]

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

        # blocks have a primary key of 'GEOID10' while all others have
        # 'GEOID' as a pk, thus the slicing
        if f_name[:5] == TIGER_PK.upper():
            attr_col.primary_key = True
            pk_col = col_name

        columns.append(attr_col)

    # add a foreign key to the ACS data unless options indicate not to, blocks
    # (pk of 'geoid10') aren't in the ACS so can't have the constraint
    if go.foreign_key and pk_col == TIGER_PK:
        meta_tables = go.metadata.tables
        geoheaders = [meta_tables[t] for t in meta_tables if GEOHEADER in t]

        for gh in geoheaders:
            foreign_col = gh.columns[TIGER_GEOID]
            fk = ForeignKeyConstraint([pk_col], [foreign_col])
            columns.append(fk)

    table = Table(
        table_name,
        go.metadata,
        *columns)
    table.create()

    return table


def transform_geometry():
    """"""



def create_transformation(src_srs, dst_srs):
    """"""

    transformation = partial(
        pyproj.transform,
        pyproj.Proj(init='epsg:{}'.format(src_srs)),
        pyproj.Proj(init='epsg:{}'.format(dst_srs), preserve_units=True)
    )

    return transformation


def process_options(arglist=None):
    """Define options that users can pass through the command line, in this
    case these are all postgres database parameters"""

    parser = utils.add_census_options(ArgumentParser(), TIGER_MOD)
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
        '-t', '--transform',
        default=None,
        type=int,
        dest='epsg',
        help='TIGER data comes from the census bureau in the projection'
             '4269, pass an EPSG code to this parameter to transform'
             'the geometry to another spatial reference system'
    )
    parser.add_argument(
        '-nfk', '--no_foreign_key',
        default=True,
        dest='foreign_key',
        action='store_false',
        help='by default a foreign key to the ACS data is created if that'
             'data exists, use this flag to disable that constraint'
    )
    parser = utils.add_postgres_options(parser)

    parser.set_defaults()
    options = parser.parse_args(arglist)
    return options


def main():
    """>> python postgis_tiger.py -y 2015 -s OR WA"""

    global go  # global options (go)
    args = sys.argv[1:]
    go = process_options(args)

    pg_url = 'postgres://{user}:{pw}@{host}/{db}'.format(
        user=go.user, pw=go.password, host=go.host, db=go.dbname)

    go.engine = create_engine(pg_url)
    go.metadata = MetaData(
        bind=go.engine,
        schema='tiger{yr}'.format(yr=go.tiger_year))

    if go.tranform:


    download_tiger_data()
    create_tiger_schema(True)
    load_tiger_data()

    if go.model:
        utils.generate_model(go.metadata)


if __name__ == '__main__':
    main()
