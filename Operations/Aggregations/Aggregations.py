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

    # take a quick peek at the data before seeing the output
    # from aggregate calculations
    print('\nBase data (first 8 rows)')
    print_cols = ['code', 'name', 'continent']
    print(tableInput.to_pandas()[print_cols] \
          .sort_values(print_cols[2]) \
          .head(8))

    count_table = tableInput.select(tableInput.name.substring(0, 1).alias('letter')) \
        .distinct() \
        .select(col('letter').count.alias('count'))

    print('\ncount_table data')
    print(count_table.to_pandas())

    count_table_2 = table.sql_query("""
        SELECT COUNT(name) AS counted
        FROM countries
    """)

    print('\ncount_table_2 data')
    print(count_table_2.to_pandas())

    count_table_3 = tableInput.select(tableInput.name.substring(0, 1).alias('letter')) \
        .group_by(col('letter')) \
        .select(col('letter'), col('letter').count.alias('count')) \
        .order_by(col('letter'))

    print('\ncount_table_3 data')
    print(count_table_3.to_pandas())

    count_table_4 = table.sql_query("""
        SELECT substring(t.name,0,1) as letter, COUNT(substring(t.name,0,1)) AS counter
        FROM countries t
        GROUP BY substring(t.name,0,1)
    """)

    print('\ncount_table_4 data')
    print(count_table_4.to_pandas())


if __name__ == '__main__':
    main()
