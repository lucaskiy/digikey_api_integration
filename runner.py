from main_files.main import ApiQuery
import pandas as pd


def run(csv_path):
    api_caller = ApiQuery()
    df = pd.read_csv(csv_path)

    api_caller.get_part_number_info(df)


if __name__ == '__main__':
    polvo_path = ".\\main_files\\polvo_bom_0805.csv"
    run(csv_path=polvo_path)
