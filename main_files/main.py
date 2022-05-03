# -*- coding: utf-8 -*-
import json
import requests
from conn.oauth_connection import ApiConn
import pandas as pd
from main_files.data_treatment import DataTreatment
from main_files.database_object import PostgresObject


class ApiQuery(PostgresObject):

    def __init__(self):
        super().__init__()
        self.api_conn = ApiConn().token
        self.url = "https://api.digikey.com/Search/v3/Products/"
        self.data_treatment = DataTreatment()
        self.api_results = []

    def get_part_number_info(self):
        print("Starting main_files.py file")
        df = pd.read_csv('BOM PO 001289 .csv', sep=";")
        part_number_list = df['Part Number'].tolist()

        print(f"Ready to retrieve {len(part_number_list)} part numbers on Digikey API\n")

        for part_number in part_number_list:
            url = self.url + str(part_number)

            headers = self.get_headers()
            columns_to_query = ["ManufacturerLeadWeeks", "ProductDescription", "UnitPrice", "StandardPackage",
                                "StandardPricing", "ProductStatus", "AlternatePackaging", "ManufacturerPartNumber",
                                "Packaging", "QuantityAvailable", "Manufacturer", "DigiKeyPartNumber",
                                "MinimumOrderQuantity"]
            params = self.get_params(columns_to_query)

            request_call = self.get_request_call(url, part_number, headers, params)
            if request_call is False:
                continue
            self.api_results.append(request_call)

        if len(self.api_results) > 0:
            print(f"\nSuccessfully retrieved {len(self.api_results)} objects on API call")
            self.treat_and_save_data()

    def treat_and_save_data(self):
        print("Starting process to treat and clean API data")
        try:
            product_results, price_results = self.data_treatment.process_data(self.api_results)
            print("API data ready to be inserted on Postgres Database")

            if len(product_results) > 0:
                table_name = 'digikey_products'
                print(f"Starting process to insert {len(product_results)} new values into table -> {table_name}")
                save_data = self.save_postgres(table_name=table_name, data=product_results)
                if save_data is True:
                    print(f"Data saved successfully on table -> {table_name}")

            if len(price_results) > 0:
                table_name = 'digikey_prices'
                print(f"Starting process to insert {len(price_results)} new values into table -> {table_name}")
                save_data = self.save_postgres(table_name=table_name, data=price_results)
                if save_data is True:
                    print(f"Data saved successfully on table -> {table_name}")

            self.close_connection()

        except Exception as error:
            print(f"Something went wrong while saving data into Postgres database")
            raise error

    def get_headers(self):
        header = {"Content-type": 'application/json',
                  "X-DIGIKEY-Client-Id": "NWJC3AeIuiqLzohcHvl8cGMPl8Xf1dOG",
                  "Authorization": self.api_conn}

        return header

    @staticmethod
    def get_params(columns_to_query: list):
        if len(columns_to_query) == 0:
            return None

        params = {"includes": ",".join(columns_to_query)}
        return params

    @staticmethod
    def get_request_call(url, part_number, headers, params=None):
        try:
            res = requests.get(url=url, headers=headers, params=params)

            if res.status_code == 200:
                print(f"API query successful on part number {part_number}")
                results = json.loads(res.text)
                return results

            else:
                print(f"Failed to retrieve information from Odoo API. Code: {res.status_code} -> {str(res.content)}")
                return False

        except Exception as error:
            print(f"Something went wrong while retrieving data on part number {part_number}")
            raise error


if __name__ == '__main__':
    ApiQuery().get_part_number_info()
