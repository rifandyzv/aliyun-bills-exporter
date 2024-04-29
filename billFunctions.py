import os
import sys
import pandas as pd

from typing import List

from alibabacloud_bssopenapi20171214.client import Client as BssOpenApi20171214Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_bssopenapi20171214 import models as bss_open_api_20171214_models
from alibabacloud_tea_util import models as util_models
from alibabacloud_tea_util.client import Client as UtilClient
from datetime import datetime, timedelta

class APIClient:
    def __init__(self):
        pass

    @staticmethod
    def connect(akID, akSecret) -> BssOpenApi20171214Client:
        """
        Initialize the Client with the AccessKey of the account
        @return: Client
        @throws Exception
        """
        # The project code leakage may result in the leakage of AccessKey, posing a threat to the security of all resources under the account. The following code examples are for reference only.
        # It is recommended to use the more secure STS credential. For more credentials, please refer to: https://www.alibabacloud.com/help/en/alibaba-cloud-sdk-262060/latest/configure-credentials-378659.
        config = open_api_models.Config(
            # Required, please ensure that the environment variables ALIBABA_CLOUD_ACCESS_KEY_ID is set.,
            access_key_id=akID,
            # Required, please ensure that the environment variables ALIBABA_CLOUD_ACCESS_KEY_SECRET is set.,
            access_key_secret=akSecret
        )
        # See https://api.alibabacloud.com/product/BssOpenApi.
        config.endpoint = 'business.ap-southeast-1.aliyuncs.com'
        return BssOpenApi20171214Client(config)




def getBill(client, billing_cycle, page_size, page_num):
    # client = APIClient.connect()
    query_bill_request = bss_open_api_20171214_models.QueryBillRequest(
        billing_cycle=billing_cycle,
        page_size=page_size,
        page_num=page_num
    )
    runtime = util_models.RuntimeOptions()
    try:
        # Copy the code to run, please print the return value of the API by yourself.
        return client.query_bill_with_options(query_bill_request, runtime)
    except Exception as error:
        # Only a printing example. Please be careful about exception handling and do not ignore exceptions directly in engineering projects.
        # print error message
        print(error.message)
        # Please click on the link below for diagnosis.
        print(error.data.get("Recommend"))
        UtilClient.assert_as_string(error.message)


def incrementMonth(date_str):
    try:
        # Parse the input date string
        input_date = datetime.strptime(date_str, '%Y-%m')

        # Calculate the next month
        next_month = input_date + timedelta(days=31)

        # Format the result as "YYYY-MM"
        result = next_month.strftime('%Y-%m')
        return result
    except ValueError:
        return "Invalid date format. Please provide a valid YYYY-MM date."



def exportAllBillstoCSV(client, bill, startMonth):
    startBill = bill

    billCount = startBill.body.data.total_count

    i = 0
    while (billCount != 0) :
        print(billCount)
        tempBill = startBill.body.data.to_map()
        items = tempBill["Items"]["Item"]
        if i==0:
            df = pd.DataFrame.from_dict(items)
            
        else:
            newDf = pd.DataFrame.from_dict(items)
            df = pd.concat([df,newDf], ignore_index=True)
        startMonth = incrementMonth(startMonth)
        startBill = getBill(client, startMonth, '300', '1')
        billCount = startBill.body.data.total_count
        
        
        i+=1

    df.to_csv('./bills.csv', index=False) 
    return df

def extract_year_month(date_str):
    try:
        # Parse the input date string
        input_date = datetime.strptime(str(date_str), '%Y-%m-%d %H:%M:%S')
        
        # Extract year and month
        year_month = input_date.strftime('%Y-%m')
        return year_month
    except ValueError:
        today = datetime.today()
        return today.strftime("%Y-%m")

def exportBillperProductCSV(df):
    df = df[["AfterTaxAmount", "CommodityCode", "PipCode", "ProductName", "UsageEndTime"]]
    df['AfterTaxAmount'] = df['AfterTaxAmount'].astype(float)
    df["UsageEndTime"] = df["UsageEndTime"].apply(extract_year_month)

    df.rename(columns={'AfterTaxAmount': 'Cost', 'PipCode': 'ProductID', 'UsageEndTime': 'Month'}, inplace=True)

    products = df["ProductID"]
    products = products.drop_duplicates()
    products = products.tolist()

    for prod in products:
        filtered_df = df[df["ProductID"] == prod]
        filtered_df.to_csv(f"./output/{prod}_bill.csv", index=False)
        print(f"Filtered DataFrame saved to {prod}")


def exportBillperProductJSON(df):
    df = df[["AfterTaxAmount", "CommodityCode", "PipCode", "ProductName", "UsageEndTime"]]
    df['AfterTaxAmount'] = df['AfterTaxAmount'].astype(float)
    df["UsageEndTime"] = df["UsageEndTime"].apply(extract_year_month)

    df.rename(columns={'AfterTaxAmount': 'Cost', 'PipCode': 'ProductID', 'UsageEndTime': 'Month'}, inplace=True)

    products = df["ProductID"]
    products = products.drop_duplicates()
    products = products.tolist()

    for prod in products:
        filtered_df = df[df["ProductID"] == prod]
        filtered_df.to_json(path_or_buf=f"./json/{prod}.json", orient='records')
        # filtered_df.to_csv(f"./output/{prod}_bill.csv", index=False)
        print(f"Filtered DataFrame saved to {prod}")