import json
import pandas as pd
import awswrangler as wr
import boto3
from decimal import Decimal
import re
import os

def lambda_handler(event, context):
    url_data = (r'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv')
    data_csv = pd.read_csv(url_data)
    #data_csv['date'] =  pd.to_datetime(data_csv['date'], format='%Y/%m/%d')
    
    url_data2 = (r'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv')
    data_csv2 = pd.read_csv(url_data2)
    #data_csv2['Date'] =  pd.to_datetime(data_csv2['Date'], format='%Y/%m/%d')
    data_csv2 = data_csv2[(data_csv2['Country/Region'] == 'US')]
    data_csv2 = data_csv2.filter(items=['Date', 'Recovered'])
    data_csv2 = data_csv2.rename(columns={'Date': 'date'})
    
    data_csv = data_csv.merge(data_csv2, how='inner', on='date')
    
    #update = len(data_csv.index)
    #print(update)
    dynamoDBResource = boto3.resource('dynamodb')
    table = dynamoDBResource.Table('covid')
    #current = table.item_count
    #print(current)
    #new = update - current # number of rows updated
    #print(new)
    
    data_csv = data_csv.fillna(0)
    # convert any floats to decimals
    for i in data_csv.columns:
        datatype = data_csv[i].dtype
        if datatype == 'float64':
            data_csv[i] = data_csv[i].apply(float_to_decimal)
    # write to dynamodb
    # check if table is empty
    #if table.item_count == 0:
    #wr.dynamodb.put_df(data_csv, table_name='covid')
    #else:
        #with table.batch_writer() as batch:
        #for index, row in data_csv.iterrows():
            #batch.put_item(json.loads(row.to_json()))
            
    new = 0
            
    with table.batch_writer() as batch:
        for index, row in data_csv.iterrows():
            if not re.match('^[0-9]{4}\-[0-9]{2}\-[0-9]{2}$', row.date) or isinstance(row.cases, str) or isinstance(row.deaths, str):
                
                AWS_REGION = "us-east-1"

                ssm_client = boto3.client("ssm", region_name=AWS_REGION)
                
                try:

                    if ssm_client.get_parameter(Name='failed', WithDecryption=True) != None:
                        get_response = ssm_client.get_parameter(Name='failed', WithDecryption=True)
                        print("hit")
                    
                        new_list_parameter = ssm_client.put_parameter(
                            Name='failed',
                            Description='Used by compute-job function',
                            Value='false',
                            Type='StringList',
                            Overwrite=True,
                            Tier='Standard',
                            DataType='text'
                        )
                        get_response2= ssm_client.get_parameter(Name='failed', WithDecryption=True)
                        print(get_response2)
                        continue
                    continue
                        
                except:
                        print("Parameter not created.")
                     
                print("this code should not run")                           
                # trigger sns
                message = {"Date is malformed": row.date}
                client = boto3.client('sns')
                response = client.publish(
                    TargetArn=os.environ.get('TargetArn'),
                    Message=json.dumps({'default': json.dumps(message)}),
                    MessageStructure='json'
                )
                AWS_REGION = "us-east-1"

                ssm_client = boto3.client("ssm", region_name=AWS_REGION)
                
                new_list_parameter = ssm_client.put_parameter(
                    Name='failed',
                    Description='Used by compute-job function',
                    Value='true',
                    Type='StringList',
                    Overwrite=True,
                    Tier='Standard',
                    DataType='text'
                )
                quit()

            r = table.get_item(Key={'date' : row.date})
            if r.get('Item') == None:
                batch.put_item(json.loads(row.to_json(), parse_float=Decimal))
                new += 1
                print("put")
    
    # trigger sns
    message = {"ETL job completed. Number of rows updated": str(new)}
    client = boto3.client('sns')
    response = client.publish(
        TargetArn=os.environ.get('TargetArn'),
        Message=json.dumps({'default': json.dumps(message)}),
        MessageStructure='json'
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    
def float_to_decimal(num):
    return Decimal(str(num))