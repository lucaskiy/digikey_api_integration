# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from main_files.database_object import PostgresObject
import math


class ReportGenerator(PostgresObject):

    def __init__(self):
        super().__init__()

    def get_most_recent_prices(self, device_type):
        query = f"""
            select 
                dp.*,
                dpr.quantity_needed,
                dpr.product_status,
                dpr.product_description,
                dpr.minimum_order_quantity,
                dpr.manufacturer,
                dpr.lead_weeks
            from 
                digikey_prices dp
            inner join 
                digikey_products dpr on dp.part_number = dpr.part_number 
            where
                dp.id_date = (select max(dp1.id_date) from digikey_prices dp1 where dp.part_number = dp1.part_number)
                and dpr.hilab_device = '{device_type}'
        """
        self.cursor.execute(query)
        query_results = self.cursor.fetchall()
        columns = ['part_number', 'date', 'unit_price', 'package_quantity', 'total_price', 'quantity_available',
                   'package_type', 'quantity_needed', 'product_status', 'product_description',
                   'minimum_order_quantity', 'manufacturer', 'lead_weeks']
        df = pd.DataFrame(query_results, columns=columns)
        self.close_connection()
        return df

    def recent_prices_report_csv(self, device_type, order_quantity=None):
        df_prices = self.get_most_recent_prices(device_type)

        df_cut_tape = df_prices.loc[df_prices['package_type'] != 'TAPE & REEL (TR)']
        df_tape_reel = df_prices.loc[df_prices['package_type'] == 'TAPE & REEL (TR)']

        if order_quantity is not None:
            df_cut_tape = df_cut_tape.reset_index()
            # create column order_quantity, multiplying the order quantity with the quantity needed of each part number
            df_cut_tape.loc[:, 'order_quantity'] = [order_quantity * quantity_needed if int(order_quantity) > 0
                                                    else None for quantity_needed in df_cut_tape['quantity_needed']]
            # create column order total price, multiplying order quantity with unit price
            df_cut_tape.loc[:, 'order_total_price'] = [unit_price * order_quantity if int(order_quantity) > 0 else None
                                                       for unit_price, order_quantity
                                                       in zip(df_cut_tape['unit_price'], df_cut_tape['order_quantity'])]

            df_tape_reel = df_tape_reel.reset_index()
            # create column order_quantity, dividing order quantity with the tape_reel package quantity
            df_tape_reel.loc[:, 'order_quantity'] = [math.ceil(order_quantity/n) if int(n) > 0 else None
                                                     for n in df_tape_reel['package_quantity']]
            # create column order total price, multiplying order quantity with unit price and package quantity
            df_tape_reel.loc[:, 'order_total_price'] = [unit_price * (order_quantity * package_quantity)
                                                        if order_quantity != np.NaN else None
                                                        for unit_price, order_quantity, package_quantity
                                                        in zip(df_tape_reel['unit_price'],
                                                               df_tape_reel['order_quantity'],
                                                               df_tape_reel['package_quantity'])]

        # change columns order
        df_cut_tape = df_cut_tape[['part_number', 'date', 'product_description', 'package_quantity', 'unit_price',
                                   'order_quantity', 'order_total_price', 'quantity_needed', 'minimum_order_quantity',
                                   'quantity_available', 'package_type', 'product_status', 'lead_weeks']]
        df_tape_reel = df_tape_reel[['part_number', 'date', 'product_description', 'package_quantity', 'unit_price',
                                     'order_quantity', 'order_total_price', 'quantity_needed', 'minimum_order_quantity',
                                     'quantity_available', 'package_type', 'product_status', 'lead_weeks']]

        # get the row with the approximate value from column 'package_quantity' and order_quantity variable
        df_cut_tape[:, 'row_approx_value'] = [package_quantity for package_quantity in df_cut_tape['package_quantity'].sub(order_quantity).abs().idxmin()]
            # df_cut_tape['package_quantity'].sub(order_quantity).abs().idxmin()
        df_cut_tape_line = df_cut_tape.iloc[[]]
        # order values by total price for each part number
        df_cut_tape = df_cut_tape.sort_values(by=['part_number', 'order_total_price'])
        df_tape_reel = df_tape_reel.sort_values(by=['part_number', 'order_total_price'])

        # create main report file, where there will by only one line of the cut_tape price (using df_cut_tape_line) \
        # and the tape_reel price
        main_report = pd.concat([df_cut_tape_line, df_tape_reel])
        main_report = main_report.drop_duplicates(subset=['part_number', 'package_quantity', 'package_type'])

        self.save_xlsx(dataframes_list=[main_report, df_tape_reel, df_cut_tape], xlsx_path='teste.xlsx')

    @staticmethod
    def save_xlsx(dataframes_list, xlsx_path):
        writer = pd.ExcelWriter(xlsx_path)
        for n, df in enumerate(dataframes_list):
            df.to_excel(writer, 'sheet%s' % n, index=False)
        writer.save()


if __name__ == '__main__':
    ReportGenerator().recent_prices_report_csv(order_quantity=2000, device_type='POLVO')
