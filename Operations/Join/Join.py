from pyflink.table import (
    DataTypes,
    TableEnvironment,
    EnvironmentSettings,
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

    table.execute_sql(f"""
       CREATE TABLE countries_numbers(
           c_code STRING,
           c_name STRING,
           number DOUBLE
           ) WITH ( 
           'connector' = 'filesystem',
           'path' = '../../countriesNumbers.csv',
           'format' = 'csv')
       """)

    tableInput_numbers = table.from_path('countries_numbers')

    # important to note that operations will be parallelized over
    # task slots across system cores so output will appear randomly
    # ordered and differ from run to run

    joined_tables = tableInput.join(tableInput_numbers, tableInput.name == tableInput_numbers.c_name) \
        .select(tableInput.wikipedia_link, tableInput_numbers.number) \
        .distinct()
    # analoously left_outer_join, right_outer_join

    print('\nJoined_tables data')
    print(joined_tables.to_pandas())

    joined_table_2 = table.sql_query("""
        SELECT DISTINCT wikipedia_link, number
        FROM
          countries s 
            JOIN
          countries_numbers l ON s.name = l.c_name
    """)

    print('\njoined_table_2 data')
    print(joined_table_2.to_pandas())


if __name__ == '__main__':
    main()
