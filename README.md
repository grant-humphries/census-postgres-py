# census-postgres-py
This project downloads data from the US Census Bureau's website, creates a schema and tables within a postgres database based on census metadata, and populates those tables from the downloaded data products.  The code is written solely in python and the entire process can be executed simply calling a script from the command line and providing a few parameters to that script.

## instructions
If you don't have an existing database in which you would like a census data schema to be created, generate one from the command line using the following syntax (this assumes you have access to a postgres instance for which you have admin privileges):

```
createdb -h your_host -U your_username census
```

Once the database has been created, simply call the script similarly to the command below.  Use this command: `python postgres_acs_db.py --help` for further information on the parameters that can be supplied to the to the script.

```
python postgres_acs_db.py -y 2014 -s OR WA -p your_pg_password
```
