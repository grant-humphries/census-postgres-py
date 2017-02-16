# census-postgres-py
This project generates console scripts that download data from the US Census Bureau's website, create a schema and tables within a postgres database based on census metadata, and populate those tables with the downloaded data products.

## getting started
census-postgres-py is designed to use [buildout](https://pypi.python.org/pypi/zc.buildout) to fetch python package dependencies, if you don't have buildout installed use the following command to do so:

```bash
pip install zc.buildout
```

This adds an executable called `buildout` to your PATH environment variable which can be used to setup a project specific instance of python equiped with all of the project's required packages (amd without installing them on your system python).  To generate a python interpreter for this project navigate to the home directory and enter the command:

```bash
buildout
```

This will start a process that writes a bunch of output in your terminal, take a look at the last few lines and make sure that it has succeeded.  If that's the case a folder called `bin` will have been created that contains a python instance and a couple of console scripts.  Before you can run those scripts you must first have a postgres database to hold the census data.  If you don't have one in place generate a new database from the command line using the following syntax (this assumes you have postgres and its command line tools installed and have admin privileges):

```bash
createdb -h your_host -U your_username census
```

Now you can run the console script that will load ACS data using a command like the one below.  For further information on options available on this executable use: `postgres_acs --help` (not that this script is not added to your PATH so you'll need to use and absolute path or cd into the `bin` folder to execute it)

```bash
./bin/postgres_acs -y 2014 -s OR WA -p your_postgres_password
```

Generating and loading the tables will take at least a couple of hours.  If that successfully completes you can add the census bureau's spatial data (called TIGER) with a second console script.  Again the `--help` parameter can be used for instructions on its use and the command below would load 2015 Block Group and Tract geometries for Oregon and Washington (note that TIGER data is generally a released about a year sooner than ACS data):

```bash
./bin/postgis_tiger -y 2015 -s OR WA -dp bg t -p your_postgres_password
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
