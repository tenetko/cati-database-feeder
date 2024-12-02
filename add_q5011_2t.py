import json
import pandas as pd
import psycopg2
import zipfile
from datetime import datetime

from glob import glob


class Q5011_2TUpdater:
    IVDATE1_DATETIME_FORMAT = "%d.%m.%Y %H:%M:%S"  # 02.05.2022 15:16:18
    ISO_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"  # 2022-05-02 15:04:09

    def __init__(self):
        self.config = self.get_config()

    def get_config(self):
        with open("config.json", "r", encoding="utf-8") as input_file:
            return json.load(input_file)

    def get_dataframe(self, file_name):
        df = pd.read_excel(file_name)
        df = df.astype({"Q5011_2T": "str"})

        return df

    def is_month_incorrect(self, dataframe):
        first_row = dataframe.iloc[0]
        ivdate = first_row["IVDate1"]
        ivdate = datetime.strptime(ivdate, self.IVDATE1_DATETIME_FORMAT)

        for _, row in dataframe.iterrows():
            recruiting_date = row["Q5011_2T"]
            if pd.isna(recruiting_date) or recruiting_date == "nan":
                continue

            recruiting_date_month = int(recruiting_date[5:7])

            if ivdate.month != recruiting_date_month:
                return True

            else:
                return False

        return False

    def update_table(self, dataframe, month_is_incorrect):

        with psycopg2.connect(
            host=self.config["db_host"],
            dbname=self.config["db_name"],
            user=self.config["db_user"],
            password=self.config["db_password"],
        ) as conn:
            with conn.cursor() as cur:
                for _, row in dataframe.iterrows():
                    recruiting_date = row["Q5011_2T"]
                    # 2024/09/04 07:07:06
                    # 2024-09-04 07:07:06

                    ivdate1 = row["IVDate1"]
                    ivdate1 = datetime.strptime(ivdate1, self.IVDATE1_DATETIME_FORMAT)
                    ivdate1 = datetime.strftime(ivdate1, self.ISO_DATETIME_FORMAT)

                    if pd.isna(recruiting_date) or recruiting_date == "nan":
                        recruiting_date = ivdate1

                    elif month_is_incorrect:
                        recruiting_date = self.make_fixed_recruiting_date(recruiting_date)

                    query_parameters = {
                        "id": row["ID"],
                        "q5011_2t": recruiting_date,
                    }
                    cur.execute(
                        """
                    UPDATE recruits_log
                    SET q5011_2t = %(q5011_2t)s
                    WHERE id = %(id)s;
                    """,
                        query_parameters,
                    )
                conn.commit()

    def make_fixed_recruiting_date(self, recruiting_date):
        month = recruiting_date[5:7]
        new_month = self.get_new_month(month)

        rd_year = recruiting_date[:4]
        rd_day_and_time = recruiting_date[8:]

        new_recruiting_date = f"{rd_year}-{new_month}-{rd_day_and_time}"

        return new_recruiting_date

    def get_new_month(self, month):
        month = int(month) + 1
        month = f"{month:02d}"

        return month

    def run(self):
        for file_name in glob("./xlsx/*.zip"):
            print(file_name[7:])
            with zipfile.ZipFile(file_name, "r") as zip:
                with zip.open(zip.namelist()[0]) as excel_file:
                    dataframe = self.get_dataframe(excel_file)
                    month_is_incorrect = self.is_month_incorrect(dataframe)
                    self.update_table(dataframe, month_is_incorrect)

    def run_excel(self):
        for file_name in glob("./xlsx/*.xlsx"):
            print(file_name[7:])
            dataframe = self.get_dataframe(file_name)
            month_is_incorrect = self.is_month_incorrect(dataframe)
            self.update_table(dataframe, month_is_incorrect)


if __name__ == "__main__":
    u = Q5011_2TUpdater()
    u.run_excel()
    # u.run()