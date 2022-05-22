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

    tableInput = table.from_elements(
        elements=[
            (1,'AD', 'Andorra', 'EU', 'https: // en.wikipedia.org / wiki / Andorra'),
            (2,'AE', 'United Arab Emirates', 'AS', 'https: // en.wikipedia.org / wiki / United_Arab_Emirates'),
            (3,'AF', 'Afghanistan', 'AS', 'https: // en.wikipedia.org / wiki / Afghanistan'),
            (4,'AG', 'Antigua and Barbuda', 'NA', 'https: // en.wikipedia.org / wiki / Antigua_and_Barbuda'),
            (5,'AI', 'Anguilla', 'NA', 'https: // en.wikipedia.org / wiki / Anguilla'),
            (6,'AL', 'Albania', 'EU', 'https: // en.wikipedia.org / wiki / Albania'),
            (7,'AM', 'Armenia', 'AS', 'https: // en.wikipedia.org / wiki / Armenia'),
            (8,'AO', 'Angola', 'AF', 'https: // en.wikipedia.org / wiki / Angola'),
            (9,'AQ', 'Antarctica', 'AN', 'https: // en.wikipedia.org / wiki / Antarctica'),
            (10,'AR', 'Argentina', 'SA', 'https: // en.wikipedia.org / wiki / Argentina'),
            (11,'AS', 'American Samoa', 'OC', 'https: // en.wikipedia.org / wiki / American_Samoa'),
            (12,'AT', 'Austria', 'EU', 'https: // en.wikipedia.org / wiki / Austria'),
            (13,'AU', 'Australia', 'OC', 'https: // en.wikipedia.org / wiki / Australia'),
            (14,'AW', 'Aruba', 'NA', 'https: // en.wikipedia.org / wiki / Aruba'),
            (15,'AZ', 'Azerbaijan', 'AS', 'https: // en.wikipedia.org / wiki / Azerbaijan'),
            (16,'BA', 'Bosnia and Herzegovina', 'EU', 'https: // en.wikipedia.org / wiki / Bosnia_and_Herzegovina'),
            (17,'BB', 'Barbados', 'NA', 'https: // en.wikipedia.org / wiki / Barbados'),
            (18,'BD', 'Bangladesh', 'AS', 'https: // en.wikipedia.org / wiki / Bangladesh'),
            (19,'BE', 'Belgium', 'EU', 'https: // en.wikipedia.org / wiki / Belgium'),
            (20,'BF', 'Burkina Faso', 'AF', 'https: // en.wikipedia.org / wiki / Burkina_Faso'),
            (21,'BG', 'Bulgaria', 'EU', 'https: // en.wikipedia.org / wiki / Bulgaria'),
            (22,'BH', 'Bahrain', 'AS', 'https: // en.wikipedia.org / wiki / Bahrain'),
            (23,'BI', 'Burundi', 'AF', 'https: // en.wikipedia.org / wiki / Burundi'),
            (24,'BJ', 'Benin', 'AF', 'https: // en.wikipedia.org / wiki / Benin'),
            (25,'BL', 'Saint Barthélemy', 'NA', 'https: // en.wikipedia.org / wiki / Saint_Barthélemy'),
            (26,'BM', 'Bermuda', 'NA', 'https: // en.wikipedia.org / wiki / Bermuda'),
            (27,'BN', 'Brunei', 'AS', 'https: // en.wikipedia.org / wiki / Brunei'),
            (28,'BO', 'Bolivia', 'SA', 'https: // en.wikipedia.org / wiki / Bolivia')
        ],
        schema=['id', 'code', 'name' , 'continent', 'wiki'])

    table.create_temporary_view("countries", tableInput)

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
