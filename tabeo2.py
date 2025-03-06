import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# loading data
offers_df = pd.read_csv("C:/Users/mayan/Downloads/offers.csv")
merchants_df = pd.read_csv("C:/Users/mayan/Downloads/merchants.csv")
payment_plans_df = pd.read_csv("C:/Users/mayan/Downloads/payment_plans.csv")

# Analysing data
offers_df.nunique()
merchants_df.nunique()
payment_plans_df.nunique()

offers_df.info()
payment_plans_df.info()

# doubt1-which of the duplicate entry to keep?
# loan ID 5771, 8644, 10304 are duplicates
offers_df[offers_df['loan_id'].duplicated(keep=False)]
# loan ID 5771, 8644, 10304 are duplicates, However other variables are different(plan_id,number_of_months,interest_rate,regulated), could this be data entry error?
payment_plans_df[payment_plans_df['loan_id'].duplicated(keep=False)]

# assuming data entry error we remove duplicates and keep the most recent entry/longer tenure in payment_plans_df
offers_df2 = offers_df.drop_duplicates(subset=["loan_id"])
payment_plans_df2 = payment_plans_df.drop_duplicates(
    subset=["loan_id"], keep="last")

offers_df2[offers_df2['loan_id'].duplicated(keep=False)]
payment_plans_df2[payment_plans_df2['loan_id'].duplicated(keep=False)]

# Merge datasets
merged_df = offers_df2.merge(payment_plans_df2, on="loan_id", how="left")
merged_df = merged_df.merge(merchants_df, on="merchant_id", how="left")

# Display merged data info
merged_df.info()

# Display first few rows
merged_df.head()

# keeping copy for later access
merged_df_copy = merged_df.copy()

pd.set_option('display.max_columns', None)

merged_df.describe()

merged_df['confirmed_month'] = pd.to_datetime(
    merged_df['confirmed_month'], format='%Y%m', errors='coerce').dt.to_period('M')

# Convert date columns to datetime format
merged_df['offer_created_date'] = pd.to_datetime(
    merged_df['offer_created_date'], format='%d/%m/%Y', errors='coerce')
merged_df['offer_confirmed_date'] = pd.to_datetime(
    merged_df['offer_confirmed_date'], format='%d/%m/%Y', errors='coerce')

merged_df['offer_created_date_week'] = merged_df['offer_created_date'].dt.strftime(
    '%Y-%m-Week%U')
merged_df['offer_confirmed_date_week'] = merged_df['offer_confirmed_date'].dt.strftime(
    '%Y-%m-Week%U')


merged_df.info()
merged_df.describe()


# Fee monthly trend
# doubt 2: 2. difference in fee collected when data is grouped by offer confirmed date and confirmed month,confirmed month is recorded in previous month(compared to order confirmed date)
# fee_trend_monthly = merged_df.groupby('confirmed_month')['tabeo_fee'].sum()
# print(fee_trend_monthly)
fee_trend_monthly = merged_df.groupby(
    merged_df['offer_confirmed_date'].dt.to_period('M'))['tabeo_fee'].sum()
print(fee_trend_monthly)
No_of_loans_Confirmed_monthly = merged_df.groupby(merged_df['offer_confirmed_date'].dt.to_period('M'))[
    'tabeo_fee'].count()
print(No_of_loans_Confirmed_monthly)
No_of_offer_created_mothly = merged_df.groupby(
    merged_df['offer_created_date'].dt.to_period('M')).size()
print(No_of_offer_created_mothly)
# No_of_offer_confirmed_monthly = merged_df.groupby(
#     merged_df['offer_confirmed_date'].dt.to_period('M')).size()
# print(No_of_offer_confirmed_monthly)

fee_trend_df_m = pd.DataFrame({'Total_Fee$': fee_trend_monthly, 'No_of_offer_created_mothly': No_of_offer_created_mothly,
                              'No._of_loans_confirmed_montly': No_of_loans_Confirmed_monthly}).reset_index()
fee_trend_df_m['Success_Rate'] = fee_trend_df_m['No._of_loans_confirmed_montly'] / \
    fee_trend_df_m['No_of_offer_created_mothly']
print(fee_trend_df_m)


# Fee weekly trend
fee_trend_weekly = merged_df.groupby(merged_df['offer_confirmed_date_week'])[
    'tabeo_fee'].sum()
print(fee_trend_weekly)
No_of_loans_Confirmed_weekly = merged_df.groupby(merged_df['offer_confirmed_date_week'])[
    'tabeo_fee'].count()
print(No_of_loans_Confirmed_weekly)
No_of_offer_created_weekly = merged_df.groupby(
    merged_df['offer_created_date_week']).size()
print(No_of_offer_created_weekly)
# No_of_offer_confirmed_monthly = merged_df.groupby(
#     merged_df['offer_confirmed_date'].dt.to_period('M')).size()
# print(No_of_offer_confirmed_monthly)

fee_trend_df_w = pd.DataFrame({'Total_Fee$': fee_trend_weekly, 'No_of_offer_created_weekly': No_of_offer_created_weekly,
                              'No._of_loans_confirmed_weekly': No_of_loans_Confirmed_weekly}).reset_index()
fee_trend_df_w['Success_Rate'] = fee_trend_df_w['No._of_loans_confirmed_weekly'] / \
    fee_trend_df_w['No_of_offer_created_weekly']
print(fee_trend_df_w)


# loan status trend
loan_status_trend = merged_df.groupby(
    [merged_df['offer_created_date'].dt.to_period('M'), 'loan_status']).size().unstack()

print(loan_status_trend)


# Calculate processing time in days
merged_df['processing_time'] = (
    merged_df['offer_confirmed_date'] - merged_df['offer_created_date']).dt.days

# Group processing time by the month of offer creation
processing_time = merged_df.groupby(
    merged_df['offer_created_date'].dt.to_period('M'))['processing_time'].mean()
# Display the result
print(processing_time)


# merchant analysis
# Grouping by month and merchant_id
merchant_analysis = merged_df.groupby([merged_df['offer_created_date'].dt.to_period('M'), 'Trading Name']).agg(
    Offers_Created=('loan_id', 'count'),
    Price_Of_Goods_Sum=('price_of_goods', 'sum'),
    Price_Of_Goods_Avg=('price_of_goods', 'mean'),
    Total_Tabeo_Fee=('tabeo_fee', 'sum'),
    Avg_Processing_Time=('processing_time', 'mean')
)

merchant_analysis2 = merged_df.groupby(merged_df['Trading Name']).agg(
    Offers_Created=('loan_id', 'count'),
    Price_Of_Goods_Sum=('price_of_goods', 'sum'),
    Price_Of_Goods_Avg=('price_of_goods', 'mean'),
    Total_Tabeo_Fee=('tabeo_fee', 'sum'),
    Avg_Processing_Time=('processing_time', 'mean')
)

# Loan status distribution per merchant per month
merchant_loan_status_counts = merged_df.groupby([merged_df['offer_created_date'].dt.to_period(
    'M'), 'Trading Name', 'loan_status']).size().unstack(fill_value=0)

merchant_loan_status_counts2 = merged_df.groupby(
    [merged_df['Trading Name'], 'loan_status']).size().unstack(fill_value=0)


# status_counts = merged_df.groupby(["offer_created_month", "merchant_id", "loan_status"]).size().unstack(fill_value=0)

# # Separate loan_written and other statuses
# status_counts["loan_written"] = status_counts.get("loan_written", 0)
# status_counts["other_statuses"] = status_counts.drop(columns=["loan_written"], errors="ignore").sum(axis=1)

# # Keep only relevant columns
# merchant_performance = status_counts[["loan_written", "other_statuses"]].reset_index()


# print(merchant_performance)

# Display the final merchant analysis
print(merchant_analysis)
print(merchant_loan_status_counts)


# checking rel between price of goods and fees
price_vs_fees = merged_df.groupby(merged_df['offer_confirmed_date'].dt.to_period('M')).agg({
    'price_of_goods': 'sum',
    'tabeo_fee': 'sum'
}).reset_index()


# int rate trend- not enough data

# Ensure offer_created_month is in period format (YYYY-MM)
merged_df['offer_created_month'] = merged_df['offer_created_date'].dt.to_period(
    'M')

# # Average interest rate per month (all loans applied)
# avg_interest_rate_monthly = merged_df.groupby('offer_created_month')[
#     'interest_rate'].mean()

# # Average interest rate per month based on loan status
# avg_interest_rate_status = merged_df.groupby(['offer_created_month', 'loan_status'])[
#     'interest_rate'].mean().unstack()

# Count of regulated and non-regulated loans per month
regulated_loan_counts = merged_df.groupby(
    ['offer_created_month', 'regulated']).size().unstack(fill_value=0)

# Count of regulated and non-regulated loans per month based on loan status
regulated_loan_status_counts = merged_df.groupby(
    ['offer_created_month', 'loan_status', 'regulated']).size().unstack(fill_value=0)

# # Display results# insuuficient data
# print("Average Interest Rate Per Month:# insuuficient data")
# print(avg_interest_rate_monthly)

# print("\nAverage Interest Rate Per Month Based on Loan Status:")
# print(avg_interest_rate_status)

print("\nCount of Regulated and Non-Regulated Loans Per Month:")
print(regulated_loan_counts)

print("\nCount of Regulated and Non-Regulated Loans Per Month Based on Loan Status:")
print(regulated_loan_status_counts)


# Sample DataFrame (Replace this with your actual merged_df)
df = merged_df.copy()

# Group by 'number_of_months' and 'loan_status', then count occurrences
grouped_df = df.groupby(
    ['number_of_months', 'loan_status']).size().unstack(fill_value=0)

# Count the number of entries for each unique 'number_of_months'
entry_count = df.groupby('number_of_months').size()

# Add the entry count as a new column in the grouped DataFrame
grouped_df['total_entries'] = grouped_df.index.map(entry_count)

# Reset index to get 'number_of_months' as a column
grouped_df.reset_index(inplace=True)

# Display the result
print(grouped_df)
