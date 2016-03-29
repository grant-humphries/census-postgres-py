# census-postgres-py
This project downloads data from the US Census Bureau's website, creates a schema and tables within a postgres database based on census metadata, and populates those tables from the downloaded data products.  The code is written solely in python and the entire process can be executed simply calling a script from the command line and providing a few parameters to that script.

## getting started
If you don't have an existing database in which you would like a census data schema to be created, generate one from the command line using the following syntax (this assumes you have access to a postgres instance for which you have admin privileges):

```sh
createdb -h your_host -U your_username census
```

Once the database has been created, simply call the script similarly to the command below.  Use this command: `python postgres_acs_db.py --help` for further information on the parameters that can be supplied to the to the script.

```sh
python postgres_acs_db.py -y 2014 -s OR WA -p your_pg_password
```

## sqlalchemy model
Presently `sqlacodegen`, the python package that is used to generate this project's sqlalchemy, model doesn't support geometry, so some manual editing of the python modules it creates is required.  For each of the modules in the tiger schema(s) the following changes need to be made.  First make an import from the `geoalchemy2` package like this:

```py
from geoalchemy2 import Geometry
```

Then replace the `geom` column in all spatial tables with the following line of code (and note that the srid value should reflect the spatial reference system of the data which is `4269` unless otherwise specified):

```py
geom = Column(Geometry(geometry_type='MULTIPOLYGON', srid=4269))
```
