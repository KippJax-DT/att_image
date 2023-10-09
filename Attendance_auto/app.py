"""
The lambda_handler function reads CSV files from S3, modifies the data, converts the dates, and
exports the final dataframe as a CSV file back to S3.

:param event: The `event` parameter is the input data passed to the Lambda function. It can contain
information such as the event type, event source, and any data associated with the event
:param context: The `context` parameter is a context object provided by AWS Lambda. It contains
information about the runtime environment and the current invocation. It can be used to access the
AWS request ID, function name, and other useful information
"""
import os

import pandas as pd
from io import StringIO
import boto3
from datetime import datetime
import logging


def lambda_handler(event, context):
    
    s3 = boto3.client('s3')
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    bucket_name = os.environ.get("bucket_name")
    
    Path_impact = os.environ.get("impact_att_object")
    Path_Voice = os.environ.get("voice_att_object")
    Path_McDuff = os.environ.get("mcduff_att_object")
    
    logger.info(f"Bucket Name: {bucket_name}")
    logger.info(f"Path Impact: {Path_impact}")
    logger.info(f"Path Voice: {Path_Voice}")
    logger.info(f"Path McDuff: {Path_McDuff}")
    
    imp_obj = s3.get_object(Bucket=bucket_name, Key=Path_impact)
    imp_body = imp_obj['Body'].read().decode('utf-8')
    # Create a StringIO object from the decoded string
    body_io = StringIO(imp_body)
    # Now you can read the CSV data into a DataFrame
    Impact_df = pd.read_csv(body_io)
    
    
    voi_obj = s3.get_object(Bucket=bucket_name, Key=Path_Voice)
    voi_body = voi_obj['Body'].read().decode('utf-8')
    # Create a StringIO object from the decoded string
    body_io = StringIO(voi_body)
    # Now you can read the CSV data into a DataFrame
    Voice_df = pd.read_csv(body_io)
    
    
    mcd_obj = s3.get_object(Bucket=bucket_name, Key=Path_McDuff)
    mcd_body = mcd_obj['Body'].read().decode('utf-8')
    # Create a StringIO object from the decoded string
    body_io = StringIO(mcd_body)
    # Now you can read the CSV data into a DataFrame
    McDuff_df = pd.read_csv(body_io)
    
    print(Impact_df.head())
    print(Voice_df.head())
    print(McDuff_df.head())
    
    # modify_def function removes unnecessary rows/columns

    def modify_df(df):
        # Drop the last column (NaN column)
        df = df.iloc[:, :-1]
        df.columns = df.iloc[0]
        df = df.drop(df.index[0])
        df = df.drop(df.tail(3).index, axis=0)
        # Handle non-numeric values
        df.replace("-", "", inplace=True)
        df = df.drop(["Student", "Grade", "Absences"], axis=1)
        return df
        
    new_1271 = modify_df(Voice_df)
    new_5981 = modify_df(Impact_df)
    new_5901 = modify_df(McDuff_df)
    
    df = pd.concat(objs = [new_1271, new_5981, new_5901])
    
    print(df.head())

    # # Gets dates from dataframe obj.
    dates = list(df.columns.drop(["Student ID"]))
    
    # Using the .melt() method to reshape columns to rows.
    df = df.melt(id_vars=["Student ID"], var_name="Date", value_name="attendance_flag")
    df["attendance_flag"] = df["attendance_flag"].replace(
        {"-": None}
    )  # replace - and ? with None.

    # Gets rid of the ="" to make our conversion easier.
    df["Date"] = df["Date"].map(lambda date: date.lstrip('="').rstrip('"'))

    # #Get the Dates from our dateframe
    dateValues = df.loc[:, "Date"]

    #   Due to the error of bugs reagarding leap year dates, we can find ways to move around such. Here we can change them into string formatted elements
    dateValues = dateValues.astype(str)

    # We would now initiate the inclusion of the current time and date as factors to make thios easier for automation
    current_month = datetime.now().month
    current_year = datetime.now().year
    next_year = datetime.now().year + 1
    prev_year = datetime.now().year - 1


    #  We can now include the time frame where our data will only be limited to. These include our months and what years they will be
    #  coresponding to our current time.

    beginning_months = [8,9,10,11,12]

    ending_months = [1,2,3,4,5,6]

    # We create an algorithm that splits all elements one by one, check to see if the dates are in the end of the year or start of the year based on their month.
    # We would also like to compare these months and dates with the current time slot to be sure that the dates do not change or need to be hardcoded in and we forget about them

    def making_dates(first_list):
        returning_list = []     # This is the output we get from this
        for i in (first_list):                  # Every element in the list, we go through with the following
            month_of_date = int(i.split("/")[0])            # Find the first element prior to the first slash mark making this the month of date.
            if month_of_date in beginning_months and current_month in beginning_months: # if date is in beginnning, and we are now in beginning, then we show the current year
                real_date = f"{i}/{current_year}"
                returning_list.append(real_date)
            elif month_of_date in ending_months and current_month in beginning_months:# if date is in ending, and we are now in beginning, then we show the current year +1
                real_date = f"{i}/{next_year}"
                returning_list.append(real_date)
            elif month_of_date in beginning_months and current_month in ending_months: # if date is in beginning, and we are now in ending, then we show the current year -1
                real_date = f"{i}/{prev_year}"
                returning_list.append(real_date)
            elif month_of_date in ending_months and current_month in ending_months: #  if date is in ending, and we are now in ending, then we show the current year
                real_date = f"{i}/{current_year}"
                returning_list.append(real_date)
            else:
                pass
        return returning_list
    # Apply this function to the list of dates to have them formatted correctly
    real_dates = making_dates(dateValues)

    # We now attempt to convert the dates that are the string
    dates_list = []

    for day in real_dates:
        correct_formatted_days = datetime.strptime(day, "%m/%d/%Y")
        formatted_day = correct_formatted_days.strftime("%m/%d/%Y")
        dates_list.append(formatted_day)

    # #Apply the results to our date column values.
    df["Date"] = dates_list

    print(f"dataset has {df.shape[0]} records.")

    df.columns = ["student_id", "date", "attendance_flag"]
    print(df)
    # Sort our data
    df = df.sort_values(by=["student_id", "date"], ascending=True)
    # Export df to csv
    
    # Finalized dataframe
    
    # upload_bucket
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    
    s3_resource = boto3.resource('s3')
    upload_bucket = os.environ.get("upload_bucket")

    s3_resource.Object(upload_bucket, 'attendance_chart_2024.csv').put(Body=csv_buffer.getvalue())



    print("Attendance deposited as Attendance_chart_2024 in the enrollment files bucket")
