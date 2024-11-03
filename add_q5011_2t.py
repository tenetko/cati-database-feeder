import json
import pandas as pd
import psycopg2
import zipfile
import datetime

from glob import glob


class Q5011_2TUpdater:
    Q5011_2T_DATETIME_FORMAT = "%Y/%m/%d %H:%M:%S"  # 2024/09/04 07:07:06
    ISO_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    def __init__(self):
        self.config = self.get_config()

    def get_config(self):
        with open("config.json", "r", encoding="utf-8") as input_file:
            return json.load(input_file)

    def get_dataframe(self, file_name):
        return pd.read_excel(file_name)

    def update_table(self, dataframe):
        with psycopg2.connect(
            host=self.config["db_host"],
            dbname=self.config["db_name"],
            user=self.config["db_user"],
            password=self.config["db_password"],
        ) as conn:
            with conn.cursor() as cur:
                for _, row in dataframe.iterrows():
                    recruiting_date = datetime.strptime(row["Q5011_2T"], self.Q5011_2T_DATETIME_FORMAT)
                    recruiting_date = datetime.strftime(recruiting_date, self.ISO_DATETIME_FORMAT)
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

    def run(self):
        for file_name in glob("./xlsx/*.zip"):
            print(file_name[7:])
            with zipfile.ZipFile(file_name, 'r') as zip:
                with zip.open(zip.namelist()[0]) as excel_file:
                    dataframe = self.get_dataframe(excel_file)
                    self.update_table(dataframe)


if __name__ == "__main__":
    u = Q5011_2TUpdater()
    u.run()