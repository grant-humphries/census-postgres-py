import os
import re
import csv
import sys
from copy import deepcopy
from argparse import ArgumentParser
from zipfile import ZipFile
from collections import defaultdict, OrderedDict
from os.path import dirname, exists, join

import xlrd
from sqlalchemy import create_engine, Column,\
    ForeignKeyConstraint, MetaData, Numeric, Table, Text

import censuspgsql.utilities as utils
from censuspgsql.utilities import ACS_MOD, ACS_SPANS, GEOHEADER, TIGER_GEOID

ACS_PRIMARY_KEY = OrderedDict([
    ('stusab', 'State Postal Abbreviation'),
    ('logrecno', 'Logical Record Number')
])

# geography groupings offered by the Census Bureau
ACS_GEOGRAPHY = [
    'Tracts_Block_Groups_Only',
    'All_Geographies_Not_Tracts_Block_Groups'
]


def download_acs_data():
    """"""

    print 'data will be downloaded to the following directory:'
    print ops.data_dir

    # get raw census data in text delimited form, the data has been
    # grouped into what the Census Bureau calls 'sequences'
    acs_url = 'http://www2.census.gov/programs-surveys/' \
              'acs/summary_file/{yr}'.format(yr=ops.acs_year)

    for geog in ACS_GEOGRAPHY:
        geog_dir = join(ops.data_dir, geog.lower())

        if not exists(geog_dir):
            os.makedirs(geog_dir)

        for st in ops.states:
            st_name = ops.state_names[st]
            geog_url = '{base_url}/data/{span}_year_by_state/' \
                       '{state}_{geography}.zip'.format(
                            base_url=acs_url, span=ops.span,
                            state=st_name, geography=geog)

            geog_path = utils.download_with_progress(geog_url, geog_dir)
            with ZipFile(geog_path, 'r') as z:
                print '\nunzipping...'
                z.extractall(dirname(geog_path))

    # the raw csv doesn't have field names for metadata, the templates
    # downloaded below provide that (but only the geoheader metadata
    # will be used by this process)
    schema_url = '{base_url}/data/{yr}_{span}yr_' \
                 'Summary_FileTemplates.zip'.format(
                      base_url=acs_url, yr=ops.acs_year, span=ops.span)

    schema_path = utils.download_with_progress(schema_url, ops.data_dir)
    with ZipFile(schema_path, 'r') as z:
        print '\nunzipping...'
        z.extractall(dirname(schema_path))

    # download the lookup table that contains information as to how to
    # extract the ACS tables from the sequences
    lookup_url = '{base_url}/documentation/user_tools/' \
                 '{lookup}'.format(base_url=acs_url, lookup=ops.lookup_file)
    utils.download_with_progress(lookup_url, ops.data_dir)


def drop_create_acs_schema(drop_existing=False):
    """"""

    engine = ops.engine
    schema = ops.metadata.schema

    if drop_existing:
        print 'dropping schema {}...'.format(schema)

        # drop in tables in chunks so max number of locks isn't exceeded,
        # geoheader needs to be dropped last since it has a foreign key to
        # all other tables
        tbl_query = engine.execute(
            "SELECT tablename FROM pg_tables "
            "WHERE schemaname = '{0}' "
            "AND tablename != '{1}';".format(schema, GEOHEADER))
        tbl_list = [t[0] for t in tbl_query]

        step = 500
        drop_template = "DROP TABLE {} CASCADE;"
        for start_ix in xrange(0, len(tbl_list), step):
            end_ix = start_ix + step
            if end_ix >= len(tbl_list):
                end_ix = None

            drops = tbl_list[start_ix: end_ix]
            drop_str = ', '.join(['{0}.{1}'.format(schema, t) for t in drops])
            drop_cmd = drop_template.format(drop_str)
            engine.execute(drop_cmd)

        engine.execute("DROP SCHEMA IF EXISTS {} CASCADE;".format(schema))

    engine.execute("CREATE SCHEMA {};".format(schema))


def create_geoheader():
    """"""

    geo_xls = '{yr}_SFGeoFileTemplate.xls'.format(yr=ops.acs_year)
    geo_schema = join(ops.data_dir, geo_xls)
    book = xlrd.open_workbook(geo_schema)
    sheet = book.sheet_by_index(0)

    columns = []
    blank_counter = 1
    for cx in xrange(sheet.ncols):
        # there are multiple fields called 'blank' that are reserved
        # for future use, but columns in the same table cannot have
        # the same name
        col_name = sheet.cell_value(0, cx).lower()
        if col_name == 'blank':
            col_name += str(blank_counter)
            blank_counter += 1

        cur_col = Column(
            name=col_name,
            type_=Text,
            doc=sheet.cell_value(1, cx).encode('utf8')
        )
        if cur_col.name.lower() in ACS_PRIMARY_KEY:
            cur_col.primary_key = True
        else:
            cur_col.primary = False

        columns.append(cur_col)

    # The 'geoid' field that exists within tiger shapefiles is a
    # truncated version of the full census geoid, this column will hold
    # the truncated version
    tiger_geoid = Column(
        name=TIGER_GEOID,
        type_=Text,
        doc='Truncated version of geoid used to join with '
            'to tables derived from TIGER shapefiles',
        unique=True,
        index=True
    )
    columns.append(tiger_geoid)

    tbl_name = GEOHEADER
    tbl_comment = 'Intermediary table used to join ACS and TIGER data'
    table = Table(
        tbl_name,
        ops.metadata,
        *columns,
        info=tbl_comment)

    print '\ncreating geoheader...'
    table.create()
    add_database_comments(table)

    # prep to populate tiger geoid column
    field_names = [c.name for c in columns]
    geoid_ix = field_names.index('geoid')
    component_ix = field_names.index('component')
    sumlevel_ix = field_names.index('sumlevel')
    tiger_ix = field_names.index(TIGER_GEOID)

    # these summary levels are excluded from the tiger_geoid because
    # their values are not unique from each other, sumlevels 050 and 160
    # also conflict with these, but remain unique if these are excluded
    sumlev_exclude = [
        '320', '610', '612',
        '620', '622', '795',
        '950', '960', '970'
    ]

    # more info on summary levels here:
    # http://www2.census.gov/programs-surveys/acs/summary_file/2014/
    # documentation/tech_docs/ACS_2014_SF_5YR_Appendices.xls

    geog_dir = join(ops.data_dir, ACS_GEOGRAPHY[0].lower())
    for st in ops.states:
        geo_csv = 'g{yr}{span}{state}.csv'.format(
            yr=ops.acs_year, span=ops.span, state=st.lower()
        )
        with open(join(geog_dir, geo_csv)) as geo_data:
            reader = csv.reader(geo_data)
            for row in reader:
                # a component value of '00' means total population, all
                # other values are subsets of the population
                tiger = None
                comp, sumlev = row[component_ix], row[sumlevel_ix]
                if comp == '00' and sumlev not in sumlev_exclude:
                    tiger = (re.match('\w*US(\w*)', row[geoid_ix]).group(1))

                row.insert(tiger_ix, tiger)

                # null values come in from the csv as empty strings
                # this converts them such that they will be NULL in
                # the database
                null_row = [None if v == '' else v for v in row]
                table.insert(null_row).execute()


def create_acs_tables():
    """"""

    acs_tables = dict()
    lookup_path = join(ops.data_dir, ops.lookup_file)

    # this csv is encoded as cp1252 (aka windows-1252) this some of the
    # strings contain characters that need to be decoded as such
    with open(lookup_path) as lookup:
        reader = csv.DictReader(lookup)
        for row in reader:
            if row['Start Position'].isdigit():
                meta_table = {
                    'name': row['Table ID'].lower(),
                    'sequence': row['Sequence Number'],
                    'start_ix': int(row['Start Position']) - 1,
                    'cells': int(''.join(
                        [i for i in row['Total Cells in Table']
                         if i.isdigit()])),
                    'comment': row['Table Title'],
                    'columns': [
                        Column(
                            name=k,
                            type_=Text,
                            doc=v,
                            primary_key=True
                        ) for k, v in ACS_PRIMARY_KEY.items()
                    ]
                }
                acs_tables[row['Table ID']] = meta_table

            # the universe of the table subject matter is stored in a
            # separate row, add it to the table comment
            elif not row['Line Number'].strip() \
                    and not row['Start Position'].strip():
                cur_tbl = acs_tables[row['Table ID']]
                cur_tbl['comment'] += ', {}'.format(row['Table Title'])

            # note that there are some rows with a line number of '0.5'
            # I'm not totally clear on what purpose they serve, but they
            # are not row in the tables and are being excluded here.
            elif row['Line Number'].isdigit():
                cur_tbl = acs_tables[row['Table ID']]
                cur_col = Column(
                    name='f' + row['Line Number'],
                    type_=Numeric,
                    doc=row['Table Title']
                )
                cur_tbl['columns'].append(cur_col)

    # a few values need to be scrubbed in the source data, this
    # dictionary defines those mappings
    scrub_map = {k.lower(): k for k in ops.state_names.keys()}
    scrub_map.update({
        '': None,
        '.': 0
    })

    # the stusab, logrecno combo is a primary key to all tables and
    # those two in geoheader serve as a foreign key to the others
    stusab_ix, logrec_ix = 2, 5
    foreign_key = ForeignKeyConstraint(
        ACS_PRIMARY_KEY.keys(),
        ['{0}.{1}'.format(GEOHEADER, k) for k in ACS_PRIMARY_KEY.keys()]
    )

    print '\ncreating acs tables, this will take awhile...'
    print 'tables completed:'

    tbl_count = 0
    for mt in acs_tables.values():
        # columns and foreign keys are accepted as *args for table object
        mt['columns'].append(deepcopy(foreign_key))

        # there are two variants for each table one contains the actual
        # data and other contains the corresponding margin of error for
        # each cell
        table_variant = {
            'standard': {
                'file_char': 'e',
                'name_ext': '',
                'meta_table': mt
            },
            'margin of error': {
                'file_char': 'm',
                'name_ext': '_moe',
                'meta_table': deepcopy(mt)}}

        for tv in table_variant.values():
            mtv = tv['meta_table']
            mtv['name'] += tv['name_ext']

            table = Table(
                mtv['name'],
                ops.metadata,
                *mtv['columns'],
                info=mtv['comment'])
            table.create()
            add_database_comments(table, 'cp1252')

            # create a list of the indices that for the columns that will
            # be extracted from the defined sequence for the current table
            columns = [stusab_ix, logrec_ix]
            columns.extend(
                xrange(mtv['start_ix'], mtv['start_ix'] + mtv['cells'])
            )

            memory_tbl = list()
            for st in ops.states:
                seq_name = '{type}{yr}{span}{state}{seq}000.txt'.format(
                    type=tv['file_char'], yr=ops.acs_year, span=ops.span,
                    state=st.lower(), seq=mtv['sequence'])

                for geog in ACS_GEOGRAPHY:
                    seq_path = join(ops.data_dir, geog, seq_name)
                    with open(seq_path) as seq:
                        reader = csv.reader(seq)
                        for row in reader:
                            tbl_ix = 0
                            tbl_row = dict()
                            for ix in columns:
                                try:
                                    row[ix] = scrub_map[row[ix]]
                                except KeyError:
                                    pass

                                field_name = mtv['columns'][tbl_ix].name
                                tbl_row[field_name] = row[ix]
                                tbl_ix += 1

                            memory_tbl.append(tbl_row)

            # this type bulk of insert uses sqlalchemy core and
            # is faster than alternative methods see details here:
            # http://docs.sqlalchemy.org/en/rel_0_8/faq.html#
            # i-m-inserting-400-000-rows-with-the-orm-and-it-s-really-slow
            ops.engine.execute(table.insert(), memory_tbl)

            # logging for user to keep track of progress
            tbl_count += 1
            if tbl_count % 50 == 0:
                sys.stdout.write(str(tbl_count))
            else:
                sys.stdout.write('.')


def add_database_comments(table, encoding=None):
    """Add comments to the supplied table and each of its columns, the
    meaning of each table and column in the ACS can be difficult to
    ascertain and this should help to clarify"""

    schema = ops.metadata.schema
    with ops.engine.begin() as connection:
        # using postgres dollar quotes on comment as some of the
        # comments contain single quotes
        tbl_comment_sql = "COMMENT ON TABLE " \
                          "{schema}.{table} IS $${comment}$$;".format(
                                schema=schema, table=table.name,
                                comment=table.info)
        connection.execute(tbl_comment_sql)

        col_template = r"COMMENT ON COLUMN " \
                       "{schema}.{table}.{column} IS $${comment}$$;"
        for c in table.columns:
            # sqlalchemy throws an error when there is a '%' sign in a
            # query thus they must be escaped with a second '%%' sign
            col_comment_sql = col_template.format(
                schema=schema, table=table.name,
                column=c.name, comment=c.doc.replace('%', '%%'))

            # the files from which comments are derived have non-ascii
            # encoded files and thus need to be appropriately decoded
            # as such
            if encoding:
                col_comment_sql = col_comment_sql.decode(encoding)

            connection.execute(col_comment_sql)


def process_options(arg_list=None):
    """"""

    parser = utils.add_census_options(ArgumentParser(), ACS_MOD)
    parser.add_argument(
        '-l', '--span', '--length',
        default=5,
        choices=ACS_SPANS,
        help='number of years that ACS data product covers'
    )
    parser = utils.add_postgres_options(parser)

    options = parser.parse_args(arg_list)
    return options


def generate_table_groups():
    """Tables are grouped if there first six letters are the same, this
    reduces the number of files that have to generated for the sqlalchemy
    model and thus speeds that creation process"""

    # if the table models aren't in memory reflect them
    if not ops.metadata.tables:
        ops.metadata.reflect(schema=ops.metadata.schema)

    table_groups = defaultdict(list)
    for schema_table in ops.metadata.tables:
        table = schema_table.split('.')[1]
        if table != GEOHEADER:
            key = table[:6]
        else:
            key = table
        table_groups[key].append(table)

    return table_groups


def main():
    """"""

    global ops
    args = sys.argv[1:]
    ops = process_options(args)

    pg_url = 'postgres://{user}:{pw}@{host}/{db}'.format(
        user=ops.user, pw=ops.password, host=ops.host, db=ops.dbname)

    ops.engine = create_engine(pg_url)
    ops.lookup_file = 'ACS_{span}yr_Seq_Table_Number_' \
                      'Lookup.txt'.format(span=ops.span)
    ops.metadata = MetaData(
        bind=ops.engine,
        schema='acs{yr}_{span}yr'.format(yr=ops.acs_year,
                                         span=ops.span))

    # download_acs_data()
    # drop_create_acs_schema(True)
    # create_geoheader()
    # create_acs_tables()

    if ops.model:
        utils.generate_model(ops.metadata, generate_table_groups())


if __name__ == '__main__':
    main()
