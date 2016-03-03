from setuptools import find_packages, setup

# the following packages can't be installed using buildout or pip on
# windows and must be add using other means:
# fiona, gdal, psycopg2, shapely

setup(
    name='censuspgsql',
    version='0.1.0',
    description='scripts to generate and populate schemas within '
                'a postgres database that contain census data',
    long_description=open('README.md').read(),
    url='https://github.com/grant-humphries/census-postgres-py',
    author='Grant Humphries',
    license='GPL',
    keywords='census postgres acs tiger',
    packages=find_packages(exclude=['censuspgql.model*']),
    install_requires=[
        'fiona>=1.5.1',
        'gdal>=1.11.2',
        'geoalchemy2>=0.2.6',
        'psycopg2>=2.6.1',
        'shapely>=1.5.13',
        'sqlacodegen>=1.1.6',
        'sqlalchemy>=1.0.11',
        'xlrd>=0.9.4'
    ],
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'postgres_acs = censuspgsql.postgres_acs:main',
            'postgis_tiger = censuspgsql.postgis_tiger:main'
        ]
    }
)
