from statistics import stdev, mean

from pyflink.common import Row
from pyflink.table import (
    DataTypes,
    TableEnvironment,
    EnvironmentSettings,
    CsvTableSource
)
from pyflink.table.udf import udf


@udf(result_type=DataTypes.ROW([DataTypes.FIELD('code', DataTypes.STRING()),
                                DataTypes.FIELD('total_length', DataTypes.INT()),
                                DataTypes.FIELD('qtr_avg', DataTypes.DOUBLE()),
                                DataTypes.FIELD('qtr_stdev', DataTypes.DOUBLE())]))
def sales_summary_stats(country: Row) -> Row:
    code, name, continent, wiki = country
    country_stats = (len(code), len(name), len(continent), len(wiki))
    total_length = sum(country_stats)
    qtr_avg = round(mean(country_stats), 2)
    qtr_stdev = round(stdev(country_stats), 2)
    return Row(code, total_length, qtr_avg, qtr_stdev)


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

    print('\nCountries Schema')
    tableInput.print_schema()

    country_stats = tableInput.map(sales_summary_stats).alias('code',
                                                    'total_length',
                                                    'quarter_avg',
                                                    'quarter_stdev')

    print('\nSales Summary Stats schema')
    country_stats.print_schema()

    print('\nSales Summary Stats data')
    print(country_stats.to_pandas().sort_values('code'))

if __name__ == '__main__':
    main()
