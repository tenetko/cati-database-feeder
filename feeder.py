import json
import zipfile
from datetime import datetime
from io import BytesIO
from time import sleep

import numpy as np
import pandas as pd
import psycopg2
import requests


class RecruitsUploader:
    def __init__(self):
        self.config = self.get_config()
        self.headers = {"SS-Token": self.config["api_token"], "Content-type": "application/json"}

    def get_config(self):
        with open("config.json", "r", encoding="utf-8") as input_file:
            return json.load(input_file)

    def get_project_id(self):
        url = f"https://api.survey-studio.com/projects?PageSize=100&PageNumber=1"
        response = requests.get(url, headers=self.headers).json()
        page_count = response["pageCount"]
        projects = response["body"]

        if page_count > 1:
            for page_number in range(2, page_count + 1):
                url = f"https://api.survey-studio.com/projects?PageSize=100&PageNumber={page_number}"
                response = requests.get(url, headers=self.headers).json()
                projects += response["body"]

        for project in projects:
            if project["name"] == self.config["project_name"]:
                return project["id"]

    def get_counter_id(self, project_id):
        url = f"https://api.survey-studio.com/projects/{project_id}/counters"
        response = requests.get(url, headers=self.headers).json()
        counters = response["body"]

        for counter in counters:
            if counter["name"] == "--------РЕКРУТ":
                return counter["id"]

    def create_results_request(self, project_id, counter_id):
        url = f"https://api.survey-studio.com/projects/{project_id}/results/data"
        data = f"""
        {{
          "counterId": {counter_id},
          "exportFormat": 2,
          "spssEncoding": 0,
          "dateFrom": null,
          "dateTo": null,
          "includeAll": false,
          "addNumericPublicId": false,
          "allowFullSizeStrings": false,
          "exportQuestionText": false,
          "exportLabelsInsteadValues": false,
          "exportLabelsAndCodeValues": true,
          "ignoreErrors": false,
          "exportHostAddress": false,
          "exportUserAgent": false,
          "exportInterviewDumpUrl": false,
          "exportInterviewResult": true,
          "exportContactData": true,
          "exportValidationComments": false,
          "exportValidationDetails": false,
          "includeTotalDurations": false,
          "exportEndedCreatedDifference": false,
          "exportContractorInfo": false,
          "convertMultiLineTextToSingleLine": false,
          "exportSpoofingDataFields": false,
          "exportMobileAppId": false,
          "exportDurationInMinutes": false,
          "exportQuestionsDuration": false,
          "exportUpdatedAt": false,
          "archiveSingleXlsxResultFile": true,
          "easyTabsIntegration": false
        }}
        """

        response = requests.post(url, headers=self.headers, data=data).json()
        return response["body"]

    def get_results(self, project_id, request_id):
        url = f"https://api.survey-studio.com/projects/{project_id}/results/data/{request_id}"
        file_url = ""

        response = requests.get(url, headers=self.headers)
        response_dict = response.json()

        if response.status_code == 200:
            if log := response_dict["body"]["log"]:
                log = log.split("\n")
                log.remove("")
                for record in log:
                    print(record)

        while True:
            sleep(10)
            response = requests.get(url, headers=self.headers)
            response_dict = response.json()
            if response.status_code == 200:
                if log := response_dict["body"]["log"]:
                    log = log.split("\n")
                    log.remove("")
                    print(log[-1])

            if response_dict["body"]["state"] == 3:
                file_url = response_dict["body"]["fileUrl"]
                break

        print(file_url)
        response = requests.get(file_url)
        with zipfile.ZipFile(BytesIO(response.content)) as zip:
            with zip.open(zip.namelist()[0]) as input_file:
                df = pd.read_excel(input_file, engine="openpyxl")
                return df

    def get_wave_number(self):
        return self.config["project_name"][-2:]

    def get_existing_phone_numbers(self, wave_number):
        existing_phone_numbers = []

        with psycopg2.connect(
            host=self.config["db_host"],
            dbname=self.config["db_name"],
            user=self.config["db_user"],
            password=self.config["db_password"],
        ) as conn:
            with conn.cursor() as cur:
                query = f"select phone from recruits_log where wave = {wave_number};"
                cur.execute(query)
                res = cur.fetchall()
                for entry in res:
                    existing_phone_numbers.append(entry[0])

        return existing_phone_numbers

    def insert_data_into_database(self, results, wave_number, existing_phone_numbers):
        results = results.replace({np.nan: None})
        skipped_phone_numbers = []

        with psycopg2.connect(
            host=self.config["db_host"],
            dbname=self.config["db_name"],
            user=self.config["db_user"],
            password=self.config["db_password"],
        ) as conn:
            with conn.cursor() as cur:
                for _, row in results.iterrows():
                    phone_number = row["Phone"]
                    if phone_number in existing_phone_numbers:
                        skipped_phone_numbers.append(phone_number)
                        continue

                    try:
                        result = row["Result"]
                        if result == "Брак":
                            skipped_phone_numbers.append(phone_number)
                            continue

                        if result == "Полное":
                            status = "Комплит"
                        else:
                            status = "Прервано"

                        date = datetime.strptime(row["IVDate1"], "%d.%m.%Y %H:%M:%S")
                        date = datetime.strftime(date, "%Y-%m-%d")

                        db_reward = row.get("DB_Reward", None)
                        db_rew = row.get("DB_Rew", None)
                        reward = row.get("Reward", None)

                        age = row["AGE"]
                        if age and age > 32767:
                            age = 32767

                        query_parameters = {
                            "id": row["ID"],
                            "wave": wave_number,
                            "status": status,
                            "phone": phone_number,
                            "result": result,
                            "ext_id": row["ExtID"],
                            "region_name": row["DB_RegionName"],
                            "operator_name": row["DB_OperatorName"],
                            "region": row["DB_Region"],
                            "operator": row["DB_Operator"],
                            "call_interval_begin": row["DB_CallIntervalBegin"],
                            "call_interval_end": row["DB_CallIntervalEnd"],
                            "time_difference": row["DB_TimeDifference"],
                            "q3_label": row["Q3_label"],
                            "q3_1": row["Q3.1"],
                            "q3_1_label": row["Q3.1_label"],
                            "q3_2": row["Q3.2"],
                            "q3_2_label": row["Q3.2_label"],
                            "s_sex": row["S_SEX"],
                            "s_sex_label": row["S_SEX_label"],
                            "name_rec": row["Q2"][:100],
                            "age_rec1": age,
                            "age_rec2": row["S_AGE_label"],
                            "q9_1": row["Q9.1"],
                            "q10": row["Q10"],
                            "q11": row["Q11"],
                            "q11_label": row["Q11_label"],
                            "q11_8t": row["Q11_8T"],
                            "q_region": row["QREGION"],
                            "q_region_label": row["QREGION_label"],
                            "q_oper_code": row["Q4"],
                            "q_oper_name": row["Q4_label"],
                            "db_reward": db_reward,
                            "db_rew": db_rew,
                            "reward": reward,
                            "q_city": row["d2006_label"],
                            "q_obrazovanie": row["d2003_label"],
                            "q_rabota": row["d2005_label"],
                            "q_dohod": row["q84_label"],
                            "date": date,
                        }
                    except KeyError as e:
                        print(f"Project name: {self.config['project_name']}")
                        print(e)

                    cur.execute(
                        """
                    INSERT INTO recruits_log
                    VALUES (
                    %(id)s, %(wave)s, %(status)s, %(phone)s, %(result)s, %(ext_id)s, %(region_name)s,
                    %(operator_name)s, %(region)s, %(operator)s, %(call_interval_begin)s, %(call_interval_end)s,
                    %(time_difference)s, %(q3_label)s, %(q3_1)s, %(q3_1_label)s, %(q3_2)s, %(q3_2_label)s,
                    %(s_sex)s, %(s_sex_label)s, %(name_rec)s, %(age_rec1)s, %(age_rec2)s, %(q9_1)s, %(q10)s,
                    %(q11)s, %(q11_label)s, %(q11_8t)s, %(q_region)s, %(q_region_label)s, %(q_oper_code)s,
                    %(q_oper_name)s, %(db_reward)s, %(db_rew)s, %(reward)s, %(q_city)s, %(q_obrazovanie)s, %(q_rabota)s, %(q_dohod)s, %(date)s)
                    """,
                        query_parameters,
                    )
                conn.commit()

        print("These phone numbers already exist in the table and therefore they were skipped:")
        print(skipped_phone_numbers)

    def run(self):
        project_id = self.get_project_id()
        print(f"Project ID: {project_id}")
        counter_id = self.get_counter_id(project_id)
        print(f"Counter ID: {counter_id}")
        request_id = self.create_results_request(project_id, counter_id)
        print(f"Request ID: {request_id}")
        sleep(10)
        results = self.get_results(project_id, request_id)
        wave_number = self.get_wave_number()
        existing_phone_numbers = self.get_existing_phone_numbers(wave_number)
        self.insert_data_into_database(results, wave_number, existing_phone_numbers)


if __name__ == "__main__":
    u = RecruitsUploader()
    u.run()
