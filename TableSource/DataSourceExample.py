from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes

def main():
    settings = EnvironmentSettings.in_batch_mode()
    tbl_env = TableEnvironment.create(settings)

    products = [
        ('Mleko',   3.99),
        ('Chleb', 5.99),
        ('Mąka',   4.99),
        ('Jajka', 14.99)
    ]

    # simple table
    simple_data = tbl_env.from_elements(products)

    print('\nsimple_data schema')
    simple_data.print_schema()

    print('\nsimple_data data')
    print(simple_data.to_pandas())

    #####################################################
    col_names = ['product', 'price']
    simple_with_column_names = tbl_env.from_elements(products, col_names)
    print('\nsimple_with_column_names schema')
    simple_with_column_names.print_schema()

    print('\nsimple_with_column_names data')
    print(simple_with_column_names.to_pandas())

    #####################################################
    schema = DataTypes.ROW([
        DataTypes.FIELD('product', DataTypes.STRING()),
        DataTypes.FIELD('price', DataTypes.DOUBLE())
    ])
    table_with_schema = tbl_env.from_elements(products, schema)

    print('\ntable_with_schema schema')
    table_with_schema.print_schema()

    print('\ntable_with_schema data')
    print(table_with_schema.to_pandas())

if __name__ == '__main__':
    main()
