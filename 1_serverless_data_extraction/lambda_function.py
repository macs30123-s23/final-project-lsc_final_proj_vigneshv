import boto3
from botocore.exceptions import ClientError
import csv
import io
from sodapy import Socrata

def lambda_handler(event, context):
    '''
    Lambda function to get data from Chicago Data Portal.
    Data is collected for the month and year specified in event.
    Data is converted to CSV and uploaded to S3.
    '''
    # City of Chicago Data Portal API Creds
    socrata_domain = "data.cityofchicago.org"
    socrata_dataset_identifier = "wrvz-psew"
    app_token = "xxxxxxxxxxxxxxxxxxxxx" # Replace with API token obtained from Chicago Data Portal
    api_username = "xxxxxxxxx@gmail.com" # Replace with API user email ID
    api_password = "xxxxxxxx" # Replace with API Password

    client = Socrata(socrata_domain, app_token, username=api_username, password=api_password, timeout = 800)

    year = event['year']
    week = event['week']

    taxi_data = None

    while True:
        try:
            taxi_data = client.get(socrata_dataset_identifier, 
                                select = '''trip_id, taxi_id,
                                            trip_start_timestamp, trip_end_timestamp,
                                            date_extract_y(trip_start_timestamp) as trip_year,
                                            date_extract_m(trip_start_timestamp) as trip_month,
                                            date_extract_woy(trip_start_timestamp) as trip_week,
                                            trip_seconds, trip_miles,
                                            pickup_community_area, dropoff_community_area,
                                            fare, tips, tolls, extras, trip_total, payment_type, company,
                                            pickup_centroid_latitude, pickup_centroid_longitude,
                                            pickup_centroid_location,
                                            dropoff_centroid_latitude, dropoff_centroid_longitude, 
                                            dropoff_centroid_location''',
                                where = str('''date_extract_y(trip_start_timestamp) = {} AND
                                            date_extract_woy(trip_start_timestamp) = {} AND
                                            taxi_id IS NOT NULL AND
                                            trip_miles IS NOT NULL AND
                                            trip_miles > 0 AND trip_miles < 40 AND
                                            trip_seconds IS NOT NULL AND
                                            trip_seconds > 10 AND trip_seconds < 15000 AND
                                            trip_total IS NOT NULL AND
                                            trip_total < 400 AND
                                            pickup_community_area IS NOT NULL AND
                                            dropoff_community_area IS NOT NULL''').format(year, week),
                                limit = 10000000)
            break
        except:
            continue
    
    bucket_name = "chi-taxi"
    if week < 10:
        s3_key = str("chi_taxi_data_{}_0{}.csv").format(year, week)
    else:
        s3_key = str("chi_taxi_data_{}_{}.csv").format(year, week)


    csv_data = convert_to_csv(taxi_data)

    try:
        upload_to_s3(csv_data, bucket_name, s3_key)
    except ClientError as cl_err:
        print(f"Could not put CSV file in S3")

    return {
        'StatusCode': 200,
        'body': 'Data uploaded to S3 as .csv successfully!'
    }


def convert_to_csv(taxi_data):
    '''
    Function to convert data from the Chicago data portal into CSV.
    Chicago Data portal data is a list of dictionaries, with each dictionary
    corresponding to a row in the dataset.
    '''

    # creating a CSV string buffer
    csv_buffer = io.StringIO()

    fieldnames = ['trip_id', 'taxi_id', 'trip_start_timestamp', 
    'trip_end_timestamp', 'trip_year', 'trip_month', 'trip_week', 'trip_seconds', 
    'trip_miles', 'pickup_community_area', 'dropoff_community_area', 
    'fare', 'tips', 'tolls', 'extras', 'trip_total', 'payment_type', 'company', 
    'pickup_centroid_latitude', 'pickup_centroid_longitude', 'pickup_centroid_location', 
    'dropoff_centroid_latitude', 'dropoff_centroid_longitude', 'dropoff_centroid_location']
    
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames, restval="NA")
    
    writer.writeheader()
    writer.writerows(taxi_data)

    return csv_buffer.getvalue()

def upload_to_s3(csv_data, bucket_name, s3_key):
    '''
    Function to upload CSV string object to S3
    '''
    
    s3_client = boto3.client("s3")
    s3_client.put_object(Body=csv_data, Bucket = bucket_name, Key = s3_key)
