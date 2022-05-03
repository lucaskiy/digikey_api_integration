# -*- coding: utf-8 -*-
from datetime import datetime


class DataTreatment:
    NOT_NULL_FIELDS = ["ManufacturerPartNumber", "QuantityAvailable", "ProductStatus"]

    def __init__(self):
        self.prices_results = list()
        self.products_results = list()

    def process_data(self, api_data):
        for product in api_data:

            has_errors = self.check_errors(product, self.NOT_NULL_FIELDS)
            if has_errors:
                continue

            product = {k.lower(): v for k, v in product.items()}
            
            for key, value in product.copy().items():

                if type(value) == list:
                    get_prices = self.get_prices(product, key)
                    if get_prices is False:
                        del product[key]
                        continue
                    del product[key]

                if type(value) == dict:
                    product[key] = product[key]['Value'].upper().strip()

                if type(value) == str:
                    product[key] = value.upper().strip()
            del product['quantityavailable']
            del product['unitprice']
            product['id_date'] = datetime.now().strftime("%Y-%m-%d")
            product = self.change_column_names(product)
            self.products_results.append(product)

        if len(self.products_results) > 0 and len(self.prices_results) > 0:
            return self.products_results, self.prices_results

    @staticmethod
    def check_errors(product, not_null_fields):
        try:
            for field in not_null_fields:
                field_data = product.get(field)
                if field not in product or field_data is None or field_data == '':
                    print(f"{field} not in data, jumping to the next data")
                    return True
            else:
                return False

        except Exception as error:
            print("Something went wrong on check_errors method")
            raise error
    
    def get_prices(self, product, key):
        try:
            if key == 'standardpricing' and len(product[key]) > 0:
                for prices in product[key]:
                    prices['ManufacturerPartNumber'] = product.get('manufacturerpartnumber')
                    prices['QuantityAvailable'] = product.get('quantityavailable')
                    prices['id_date'] = datetime.now().strftime("%Y-%m-%d")
                    prices['package_type'] = product.get('packaging').get('Value')

                    prices = self.change_column_names(prices)
                    self.prices_results.append(prices)
                return True

            elif key == 'alternatepackaging' and len(product[key]) > 0:
                for prices in product[key]:
                    if prices['Packaging']['Value'] != 'Digi-Reel®':
                        prices_dict = {
                            'ManufacturerPartNumber': prices.get('ManufacturerPartNumber'),
                            'QuantityAvailable': prices.get('QuantityAvailable'),
                            'UnitPrice': prices.get('UnitPrice'),
                            'BreakQuantity': prices.get('MinimumOrderQuantity'),
                            'id_date': datetime.now().strftime("%Y-%m-%d"),
                            'package_type': prices.get('Packaging').get('Value').upper()
                        }
                        prices_dict['TotalPrice'] = prices_dict['UnitPrice'] * prices_dict['BreakQuantity']

                        prices_dict = self.change_column_names(prices_dict)
                        self.prices_results.append(prices_dict)
                return True
            else:
                return False

        except Exception as error:
            print("Something went wrong on get_prices method")
            print(error)
            return True

    @staticmethod
    def change_column_names(results_dict):
        change_columns = {
            'ManufacturerPartNumber': 'part_number',
            'UnitPrice': 'unit_price',
            'BreakQuantity': 'quantity',
            'TotalPrice': 'total_price',
            'QuantityAvailable': 'quantity_available',
            'standardpackage': 'standard_package',
            'manufacturerleadweeks': 'lead_weeks',
            'productstatus': 'product_status',
            'manufacturerpartnumber': 'part_number',
            'quantityavailable': 'quantity_available',
            'digikeypartnumber': 'digikey_part_number',
            'productdescription': 'product_description',
            'unitprice': 'unit_price',
            'MinimumOrderQuantity': 'minimum_order_quantity',
            'minimumorderquantity': 'minimum_order_quantity'
        }
        for key, value in results_dict.copy().items():
            if key in change_columns:
                new_key = change_columns[key]
                results_dict[new_key] = value
                del results_dict[key]

        return results_dict
