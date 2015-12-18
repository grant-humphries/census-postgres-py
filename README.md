# census-postgres-py
The scripts contained in this repo download data from the US Census Bureau's website, create a schema and tables within a postgres database based on census metadata and populate from the downloaded data products.  The code is written soley in python and the entire process can be executed simply calling a script from the command line and providing a few parameters.

## instructions
If you do not have an existing postgres database in which you would like a census data schema to be created generate one from the command line using the following syntax (this assumes you have postgres installed or can communicate with a postgres instance through the command line)

```
createdb -h your_host -U your_username census
```

Once the database has been created simply call the script similarly to as it is below.  Use this command: `python postgres_acs_db.py --help` for further information on the parameters that can be supplied to the to the script.

```
python postgres_acs_db.py -y 2014 -s OR WA -p your_pg_password
```
