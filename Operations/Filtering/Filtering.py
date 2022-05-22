from pyflink.table import (
    DataTypes,
    TableEnvironment,
    EnvironmentSettings,
    CsvTableSource
)
from pyflink.table.expressions import col


def main():
    env_settings = EnvironmentSettings.in_batch_mode()
    table = TableEnvironment.create(env_settings)

    table.execute_sql(f"""
    CREATE TABLE countries(
        code STRING,
        name STRING,
        continent STRING,
        wikipedia_link STRING
        ) WITH ( 
        'connector' = 'filesystem',
        'path' = '../../countries.csv',
        'format' = 'csv')
    """)

    tableInput = table.from_path('countries')

    # In random order!
    countriesA = tableInput.select(tableInput.code,
                                   tableInput.name.alias('nazwa')) \
        .distinct() \
        .where((col('nazwa').substring(0, 1) == 'A') | (col('nazwa').substring(0, 1) == 'D'))

    #                   .where(tableInput.name.substring(0, 1) == 'A')

    print('\nA countries')
    print(countriesA.to_pandas())

    codesA = table.sql_query("""
        SELECT DISTINCT code, name
        FROM countries 
        WHERE substring(code,0,1) = 'A'
    """)

    print('\nA codes')
    print(codesA.to_pandas())

    # filter from data API

    tableInput.filter(tableInput.name.substring(0, 1) == 'A').execute().print()

if __name__ == '__main__':
    main()
