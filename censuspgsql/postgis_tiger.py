import os
import sys
from argparse import ArgumentParser
from functools import partial
from os.path import basename, exists, join, splitext
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
from censuspgsql.utilities import ACS_SCHEMA, ACS_SPANS, \
    GEOHEADER, GEOID, PG_URL, TIGER_GEOID, TIGER_MOD

TIGER_PK = GEOID
TIGER_PRODUCT = {
    'b': 'TABBLOCK10',
    'bg': 'BG',
    't': 'TRACT'
}


def download_tiger_data(shp_path_only=False):
    """"""

    tiger_url = 'ftp://ftp2.census.gov/geo/tiger/TIGER{yr}'.format(
        yr=gv.tiger_year)

    gv.shp = dict()
    for prod in gv.product:
        prod_name = TIGER_PRODUCT[prod].lower()
        prod_class = ''.join([c for c in TIGER_PRODUCT[prod] if c.isalpha()])
        prod_dir = join(gv.data_dir, prod_class)

        if not exists(prod_dir):
            os.makedirs(prod_dir)

        for st in gv.states:
            prod_url = '{base_url}/{class_}/' \
                       'tl_{yr}_{fips}_{name}.zip'.format(
                            base_url=tiger_url, class_=prod_class,
                            yr=gv.tiger_year, fips=gv.state_fips[st],
                            name=prod_name)

            # add names of shapefiles to dictionary mapping to the
            # table that they will be inserted into
            shp_name = '{}.shp'.format(splitext(basename(prod_url))[0])
            shp_path = join(prod_dir, shp_name)
            gv.shp[shp_path] = prod

            if not shp_path_only:
                prod_path = utils.download_with_progress(prod_url, prod_dir)
                with ZipFile(prod_path, 'r') as z:
                    print '\nunzipping...'
                    z.extractall(prod_dir)


def create_tiger_schema(drop_existing=False):
    """"""

    engine = gv.engine
    schema = gv.metadata.schema

    if drop_existing:
        engine.execute("DROP SCHEMA IF EXISTS {} CASCADE;".format(schema))

    try:
        gv.engine.execute("CREATE SCHEMA {};".format(schema))
    except sqlalchemy.exc.ProgrammingError as e:
        print e.message
        print 'Data will be loaded into the existing schema,'
        print 'if you wish recreate the schema use the "drop_existing" flag'


def load_tiger_data():
    """"""

    transformation = check_epsg_for_transformation()

    # if the foreign key flag is set to true reflect geoheader tables
    # in matching acs schemas, tiger data is matched to acs that is one
    # year less recent because it is released one year sooner
    if gv.foreign_key:
        acs_year = gv.tiger_year - 1
        for i in ACS_SPANS:
            # FIXME should probably template this on global level
            acs_schema = ACS_SCHEMA.format(yr=acs_year, span=i)
            try:
                gv.metadata.reflect(schema=acs_schema, only=[GEOHEADER])
            except sqlalchemy.exc.InvalidRequestError:
                pass

    for shp_path, product in gv.shp.items():
        with fiona.open(shp_path) as tiger_shape:
            shp_metadata = tiger_shape.meta.copy()
            table = create_tiger_table(shp_metadata, product)

            print '\nloading shapefile "{0}" ' \
                  'into table: "{1}.{2}":'.format(
                       basename(shp_path), gv.metadata.schema, table.name)
            print 'features inserted:'

            memory_tbl = list()
            max_fid = max(tiger_shape.keys())
            for fid, feat in tiger_shape.items():
                fields = feat['properties']
                row = {k.lower(): v for k, v in fields.items()}

                # casting to multipolygon here because a few features
                # are multi's and the geometry types must match
                shapely_geom = MultiPolygon([shape(feat['geometry'])])

                if transformation:
                    shapely_geom = ops.transform(transformation, shapely_geom)

                # geoalchemy2 requires that geometry be in EWKT format
                # for inserts, that conversion is made below
                ga2_geom = WKTElement(shapely_geom.wkt, gv.epsg)
                row['geom'] = ga2_geom
                memory_tbl.append(row)

                count = fid + 1
                if count % 1000 == 0 or fid == max_fid:
                    gv.engine.execute(table.insert(), memory_tbl)
                    memory_tbl = list()

                    # logging to inform the user
                    if count % 20000 == 0:
                        sys.stdout.write(str(count))
                    elif fid == max_fid:
                        print '\n'
                    else:
                        sys.stdout.write('..')


def check_epsg_for_transformation():
    """"""

    # if shapefile paths haven't be stored in the global namespace
    # variable use the download function to get them
    if not gv.shp:
        download_tiger_data(shp_path_only=True)

    # get the tigers native spatial reference system code from one
    # of the tiger shapefiles
    epsg_shp = fiona.open(gv.shp.keys()[0])
    epsg_shp_meta = epsg_shp.meta.copy()
    tiger_epsg = int(epsg_shp_meta['crs']['init'].split(':')[1])

    if gv.epsg and gv.epsg != tiger_epsg:
        transformation = partial(
            pyproj.transform,
            pyproj.Proj(init='epsg:{}'.format(tiger_epsg)),
            pyproj.Proj(init='epsg:{}'.format(gv.epsg), preserve_units=True)
        )
        return transformation
    else:
        gv.epsg = tiger_epsg
        return None


def create_tiger_table(shp_metadata, product, drop_existing=False):
    """shp_metadata parameter must be a fiona metadata object"""

    # handle cases where the table already exists
    schema = gv.metadata.schema
    table_name = TIGER_PRODUCT[product].lower()
    if not drop_existing:
        engine = gv.engine
        if engine.dialect.has_table(engine.connect(), table_name, schema):
            full_name = '{0}.{1}'.format(schema, table_name)
            print 'Table {} already exists, ' \
                  'using existing table...'.format(full_name)
            print 'to recreate the table use the "drop_existing" flag'

            return gv.metadata.tables[full_name]

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
            srid=gv.epsg))
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
    if gv.foreign_key and pk_col == TIGER_PK:
        meta_tables = gv.metadata.tables
        geoheaders = [meta_tables[t] for t in meta_tables if GEOHEADER in t]

        for gh in geoheaders:
            foreign_col = gh.columns[TIGER_GEOID]
            fk = ForeignKeyConstraint([pk_col], [foreign_col])
            columns.append(fk)

    table = Table(
        table_name,
        gv.metadata,
        *columns)
    table.create()

    return table


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

    parser.set_defaults(shp=None)
    options = parser.parse_args(arglist)
    return options


def main():
    """>> python postgis_tiger.py -y 2015 -s OR WA"""

    # gv will be a namespace object that will hold all global variables
    global gv  
    args = sys.argv[1:]
    gv = process_options(args)

    pg_url = PG_URL.format(user=gv.user, pw=gv.password,
                           host=gv.host, db=gv.dbname)
    gv.engine = create_engine(pg_url)
    gv.metadata = MetaData(
        bind=gv.engine,
        schema='tiger{yr}'.format(yr=gv.tiger_year))

    download_tiger_data()
    create_tiger_schema(True)
    load_tiger_data()

    if gv.model:
        utils.generate_model(gv.metadata)


if __name__ == '__main__':
    main()
