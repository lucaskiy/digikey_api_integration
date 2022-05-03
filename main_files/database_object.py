import psycopg2
from psycopg2 import extras
from conn.database_config import config
import pandas as pd

params = config()


class PostgresObject:
    INSERT_QUERY_PRODUCTS = """
        INSERT INTO 
            %s(%s) 
        VALUES 
            %%s 
        ON CONFLICT (%s) 
        DO UPDATE SET
            id_date = EXCLUDED.id_date,
            product_status = EXCLUDED.product_status,
            lead_weeks = EXCLUDED.lead_weeks;
    """
    INSERT_QUERY_PRICES = """
        INSERT INTO 
            %s(%s) 
        VALUES 
            %%s 
        ON CONFLICT (%s) 
        DO NOTHING
    """

    def __init__(self):
        self.postgres_connection = psycopg2.connect(**params)
        self.cursor = self.postgres_connection.cursor()

    def close_connection(self):
        try:
            print("Closing connection to redshift")
            self.postgres_connection.commit()  # Ensure to finish the
            self.postgres_connection.close()
        except Exception as err:
            print(err)
            print("Error on closing connection")

    def save_postgres(self, table_name, data: list):
        df = pd.DataFrame(data)

        tuples = [tuple(value) for value in df.to_numpy()]
        cols = ','.join(list(df.columns))
        pk = self.get_primary_key(table_name)
        if table_name == 'digikey_products':
            query = self.INSERT_QUERY_PRODUCTS % (table_name, cols, pk)
        elif table_name == 'digikey_prices':
            query = self.INSERT_QUERY_PRICES % (table_name, cols, pk)
        else:
            print("No table name or invalid table name")

        try:
            extras.execute_values(self.cursor, query, tuples)
            return True

        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.postgres_connection.rollback()
            self.cursor.close()
            raise False

    def get_primary_key(self, table_name):
        query = f"""
        SELECT c.column_name, c.data_type
        FROM information_schema.table_constraints tc 
        JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
        JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
          AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
        WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = '{table_name}';
        """
        self.cursor.execute(query)
        query_results = self.cursor.fetchall()
        pk_results = [item[0] for item in query_results]
        pk = ','.join(pk_results)
        return pk
