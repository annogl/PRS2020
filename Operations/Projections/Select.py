from pyflink.table import (
    DataTypes, TableEnvironment, EnvironmentSettings,
    CsvTableSource
)


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

    # important to note that operations will be parallelized over
    # task slots across system cores so output will appear randomly
    # ordered and differ from run to run

    #####################################################
    codes = tableInput.select(tableInput.code,
                                  tableInput.name.alias('Nazwa'))

    print('\nCodes table')
    print(codes.to_pandas())

    #####################################################

    # SQL query is run on the env not table!
    codes2 = table.sql_query(f"""
        SELECT code, name AS nazwa, CHARACTER_LENGTH(name) as nameLength
        FROM countries
    """)
    print('\nCodes_2 table')
    print(codes2.to_pandas())

    print('\nSchema')
    codes2.print_schema()

    #####################################################

    codes_3 = tableInput.select(tableInput.code,
                                  tableInput.name.alias('Nazwa'))\
        .limit(10)

    print('\nCodes_3 table')
    print(codes_3.to_pandas())

    #####################################################
    # one argument so have to divide with "' '"
    codes_4 = tableInput.drop_columns("name, code")

    print('\nCodes_4 table')
    print(codes_4.to_pandas())

    print('\nSchema')
    codes_4.print_schema()




if __name__ == '__main__':
    main()
