import requests
import pandas as pd
from math import ceil
from pymongo import MongoClient
import yagmail

client = MongoClient()
client = MongoClient("mongodb://localhost:27017/")

mydb = client["test"]
mycol = mydb["subtest"]

def re_arrange(df):
  new_column = ["start_date","end_date","year","month","week","ins_ad_count","ins_ref_count","ins_ads_lead","ins_ref_lead", "loan_ad_traffic", "loan_ref_traffic", "loan_ads_lead", "loan_ref_lead"]
  df = df[new_column]
  return df

def week_of_month(dt):
    """ Returns the week of the month for the specified date.
    """

    first_day = dt.replace(day=1)

    dom = dt.day
    adjusted_dom = dom + first_day.weekday()

    return int(ceil(adjusted_dom/7.0))

def get_week_month_year(result):
  result['start_date']= pd.to_datetime(result['start_date'])
  result["month"] = result["start_date"].dt.strftime("%m")
  result["year"] = result["start_date"].dt.strftime("%Y")
  result['week'] = result['start_date'].apply(week_of_month)
  result['start_date'] = result['start_date'].dt.strftime('%Y-%m-%d')
  return result

def send_email():
    yag = yagmail.SMTP(user="usetestpython@gmail.com", password= "pP123456789")
    email_receiver = "tohlouis96@gmail.com"
    subject = "Weekly Report"
    body = "This is the report of this week"
    email_attachment = "result.xlsx"
    yag.send(to=email_receiver,subject=subject,attachments=email_attachment,contents=body)

data =  {"start_date":"2021-01-06",
   "end_date":"2021-01-06"
  }
total_in_ins_flow = requests.post( " https://dashboard-ntl-api.herokuapp.com/total_in_ins_flow", json=data)
lead_ins_count = requests.post( " https://dashboard-ntl-api.herokuapp.com/lead_ins_count", json=data)
new_inquiries_loan_m = requests.post( " https://dashboard-ntl-api.herokuapp.com/new_inquiries_loan_m", json=data)
lead_loanm_count = requests.post( " https://dashboard-ntl-api.herokuapp.com/lead_loanm_count", json=data)

total_in_ins_flow_df = pd.DataFrame([total_in_ins_flow.json()])
lead_ins_count_df = pd.DataFrame([lead_ins_count.json()])
new_inquiries_loan_m_df = pd.DataFrame([new_inquiries_loan_m.json()])
lead_loanm_count_df = pd.DataFrame([lead_loanm_count.json()])

total_in_ins = total_in_ins_flow_df[["ad_count","referral_count"]]
total_in_ins = total_in_ins.rename(columns={"ad_count":"ins_ad_count","referral_count":"ins_ref_count"})

lead_ins_c = lead_ins_count_df[["ads_lead","referral_lead"]]
lead_ins_c = lead_ins_c.rename(columns={"ads_lead":"ins_ads_lead","referral_lead":"ins_ref_lead"})

new_inquiries_loan = new_inquiries_loan_m_df[["ad_traffic","ref_traffic"]]
new_inquiries_loan  = new_inquiries_loan.rename(columns={"ad_traffic":"loan_ad_traffic","ref_traffic":"loan_ref_traffic"})

lead_loanm_c = lead_loanm_count_df[["ads_lead","referral_lead"]]
lead_loanm_c = lead_loanm_c.rename(columns={"ads_lead":"loan_ads_lead","referral_lead":"loan_ref_lead"})

date = pd.DataFrame(data,index = [0])

result = pd.concat([date,total_in_ins, lead_ins_c,new_inquiries_loan, lead_loanm_c], axis = 1)

result = get_week_month_year(result)
result = re_arrange(result)
result = result.drop(["start_date","end_date"], axis = 1)

data = result.to_dict("records")
mydb.subtest.insert_many(data)
