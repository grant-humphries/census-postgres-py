from setuptools import find_packages, setup

# the following packages can't be installed using buildout or pip on
# windows and must be add using other means:
# fiona, gdal, psycopg2, pyproj, shapely

setup(
    name='censuspgsql',
    version='0.1.0',
    author='Grant Humphries',
    description='scripts to generate and populate schemas within '
                'a postgres database that contain census data',
    entry_points={
        'console_scripts': [
            'postgres_acs = censuspgsql.postgres_acs:main',
            'postgis_tiger = censuspgsql.postgis_tiger:main',
            'sqlacodegen = sqlacodegen.main:main'
        ]
    },
    include_package_data=True,
    install_requires=[
        'appdirs>=1.4.0',
        'fiona>=1.5.1',
        'gdal>=1.11.2',
        'geoalchemy2>=0.2.6',
        'psycopg2>=2.6.1',
        'pyproj>=1.9.5.1',
        'shapely>=1.5.13',
        'sqlacodegen>=1.1.6',
        'sqlalchemy>=1.0.11',
        'xlrd>=0.9.4'
    ],
    keywords='census postgres acs tiger',
    license='GPL',
    long_description=open('README.md').read(),
    packages=find_packages(exclude=['censuspgsql.model*']),
    url='https://github.com/grant-humphries/census-postgres-py'
)
