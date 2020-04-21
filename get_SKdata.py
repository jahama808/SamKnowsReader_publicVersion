

import pandas as pd
from pandas.io.json import json_normalize

import time
from pytz import timezone
import datetime
from datetime import datetime
import requests
import json
import MySQLdb
from dbconnect import connection


# grab the ids of the whiteboxes


headers = {
    'Authorization': 'e2801dbde543564012895b7dbb34eea25ccb7bd8de6f14b2e079d53a4b605732aca5d2cddab24ab36ca3890b5fe289322b02cd468c0ec5ce0d888945d7a95441b08305c5dc358e8452c77b48a938b53bfdac400946ae18ab853901dd0c55098db538af1fd5ff57c0ce9e36ee4b6c0d8eca9761eb36f3a6ad42a275c22533508bdeec41bea08fb39801f0b0fd880b298f9dd156fcf3b382d9a5b09b0b9e651b1bda7781ac113d794f7bc1a26c02e9f4af36d9a87e61b0bb6b65b94d49111463754387057cf080143127a2b3c6ad4dac68e136a704a098ec61bcd71fb65b9122d014a822d916472cf3e5ce5964bf41f2a682e84cd39c554fb607bb545ba55cb6ecd65318fef9f80e57e6ebf1d43256b27588b5b152726c6e4c5c07d1f22be5167c1733d3f255bc4168964501a6d387a8b25f5afd192d06b668c006ba2a99dea55c28b7a0a5b885b69f43f328cef8d3e09548ba40c687241942769f39b078f024d7f447071ef81a164ffbf3ce9534b2261e5ef4fa9765ebb0b25ca8c2ae2d770f48e595deb23574153347fe7b0018a87bf2',
    'Content-Type': 'application/json',
}

data_download = '{"splits":["unit"],"metric":"httpgetmt","chartType":"aggregate","aggregation":"hourly","normalised":true,"prefilter":true,"filters":[{"id":"isp","filterType":1,"filterValues":["372"]}],"time":{"from":"-7 days","to":"now"},"agent_agg":false}'
data_upload = '{"splits":["unit"],"metric":"httppostmt","chartType":"aggregate","aggregation":"hourly","normalised":true,"prefilter":true,"filters":[{"id":"isp","filterType":1,"filterValues":["372"]}],"time":{"from":"-7 days","to":"now"},"agent_agg":false}'
data_udplatency = '{"splits":["unit"],"metric":"udpLatency","chartType":"aggregate","aggregation":"hourly","normalised":true,"prefilter":true,"filters":[{"id":"isp","filterType":1,"filterValues":["372"]}],"time":{"from":"-7 days","to":"now"},"agent_agg":false}'


#data_latency
print("Data retrieval from SK One successful")
response_download = requests.post('https://data-api.samknows.one/metric_data', headers=headers, data=data_download)
response_upload = requests.post('https://data-api.samknows.one/metric_data', headers=headers, data=data_upload)
response_latency = requests.post('https://data-api.samknows.one/metric_data', headers=headers, data=data_udplatency)

print("Data loaded into json containers")
rjson_download = response_download.json()
rjson_upload = response_upload.json()
rjson_latency = response_latency.json()


# count the number of whiteboxes

number_of_whiteboxes_download = int(len(rjson_download['data']))
print("Number of Whiteboxes (download): "+str(number_of_whiteboxes_download))



# This will take in the data and put it into a pandas data frame

# create the dataframe columns

sk_dataframe = pd.DataFrame(columns=['whitebox','time','downstream','upstream','latency'])
access_dataframe = pd.DataFrame(columns=['account','serial','unitid','accessid','coreid','accessType','failrateDownload','failrateUpload','failrateLatency'])

for x in range (1,number_of_whiteboxes_download):
    
    whitebox_ID = str(rjson_download['data'][x]['metricData'][0]['unit_id'])


    
    total_measurements = int(len(rjson_download['data'][x]['metricData']))

    
    for y in range(1,total_measurements):

        # this will grab the time
        

        date_str = rjson_download['data'][x]['metricData'][y]['dtime']
        datetime_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
        datetime_obj_pacific = timezone('Pacific/Honolulu').localize(datetime_obj)
        
        datetime_of_test = datetime_obj_pacific.strftime("%Y-%m-%d %H:%M %Z%z")
        #print(datetime_of_test)

        
        
        download_result = int(rjson_download['data'][x]['metricData'][y]['mean'])*8/1000

        sk_dataframe = sk_dataframe.append({'whitebox':whitebox_ID,'time':datetime_of_test,'downstream':download_result},ignore_index=True)



print("Download data loaded in into dataframe")

#Upload data section

# count the number of whiteboxes

number_of_whiteboxes_upload = int(len(rjson_upload['data']))
print("Number of Whiteboxes (upload): "+str(number_of_whiteboxes_upload))

for x in range (0,number_of_whiteboxes_upload):
    
    whitebox_ID = str(rjson_upload['data'][x]['metricData'][0]['unit_id'])
    

    total_measurements_upload = int(len(rjson_upload['data'][x]['metricData']))


    for y in range(0,total_measurements_upload):

        # this will grab the time
        

        date_str = rjson_upload['data'][x]['metricData'][y]['dtime']
        datetime_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
        datetime_obj_pacific = timezone('Pacific/Honolulu').localize(datetime_obj)
        
        datetime_of_test = datetime_obj_pacific.strftime("%Y-%m-%d %H:%M %Z%z")
        #print(datetime_of_test)

       
        
        upload_result = int(rjson_upload['data'][x]['metricData'][y]['mean'])*8/1000

        
        sk_dataframe.loc[(sk_dataframe['whitebox']==whitebox_ID) & (sk_dataframe['time'] == datetime_of_test),'upstream']=upload_result


print("Upload data loaded in into dataframe")

# This section grabs the latency information
# count the number of whiteboxes

number_of_whiteboxes_latency = int(len(rjson_latency['data']))
print("Number of Whiteboxes (latency): "+str(number_of_whiteboxes_latency))

for x in range (0,number_of_whiteboxes_latency):
    
    whitebox_ID = str(rjson_latency['data'][x]['metricData'][0]['unit_id'])
    

    total_measurements_latency = int(len(rjson_latency['data'][x]['metricData']))


    for y in range(0,total_measurements_latency):

        # this will grab the time
        

        date_str = rjson_latency['data'][x]['metricData'][y]['dtime']
        datetime_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
        datetime_obj_pacific = timezone('Pacific/Honolulu').localize(datetime_obj)
        
        datetime_of_test = datetime_obj_pacific.strftime("%Y-%m-%d %H:%M %Z%z")
        #print(datetime_of_test)

       
        
        latency_result = int(rjson_latency['data'][x]['metricData'][y]['mean'])/1000


        
        sk_dataframe.loc[(sk_dataframe['whitebox']==whitebox_ID) & (sk_dataframe['time'] == datetime_of_test),'latency']=latency_result


print("Latency data loaded in into dataframe")

# at this point, the dataframe now contains all the SK data.


access_data = pd.read_csv("access.csv")

size_of_sk_data = (len(sk_dataframe.index))
size_of_access_data = (len(access_data.index))



threshold = 10000 #speed in kbps
upload_threshold = 1000 #speed in kbps
latency_threshold = 100 #latency in ms



for i in range(0,size_of_access_data):
    target = str(access_data.iloc[i]['unitID'])
    total_entries = len(sk_dataframe[(sk_dataframe['whitebox']==target)])
    
    # finding the entries below the download threshold

    number_below_min  = len(sk_dataframe[(sk_dataframe['whitebox']==target) & (sk_dataframe['downstream'] < threshold)])
    low_down_df = sk_dataframe[(sk_dataframe['whitebox']==target) & (sk_dataframe['downstream'] < threshold)]

    if number_below_min > 1:
        fail_rate_dl = int(float(number_below_min)/float(total_entries)*100)
        print("whitebox: {} has {} below download threshold of {} Kbps out of {} entries.  Fail Rate: {}".format(target,number_below_min,threshold,total_entries,fail_rate_dl))
        access_data.loc[(access_data['unitID']==int(target)),'failrateDownload']= fail_rate_dl
        print(low_down_df)
    else:
        if total_entries > 0:
            fail_rate_dl = int(float(number_below_min)/float(total_entries)*100)
            access_data.loc[(access_data['unitID']==int(target)),'failrateDownload']= fail_rate_dl
        else:
            access_data.loc[(access_data['unitID']==int(target)),'failrateDownload']=  -1
        

    # finding the entries below the upload threshold

    number_below_min_upload = len(sk_dataframe[(sk_dataframe['whitebox']==target) & (sk_dataframe['upstream'] < upload_threshold)])
    low_up_df = sk_dataframe[(sk_dataframe['whitebox']==target) & (sk_dataframe['upstream'] < upload_threshold)]

    if number_below_min_upload > 1:
        fail_rate_ul = int(float(number_below_min_upload)/float(total_entries)*100)
        print("whitebox: {} has {} below upload threshold of {} Kbps out of {} entries.  Fail Rate: {}".format(target,number_below_min_upload,upload_threshold,total_entries,fail_rate_ul))
        access_data.loc[(access_data['unitID']==int(target)),'failrateUpload']= fail_rate_ul
        print(low_up_df)
    else:
        if total_entries > 0:
            fail_rate_ul = int(float(number_below_min_upload)/float(total_entries)*100)
            access_data.loc[(access_data['unitID']==int(target)),'failrateUpload']= fail_rate_ul
        else:
            access_data.loc[(access_data['unitID']==int(target)),'failrateUpload']= -1

    # finding the entries above the latency threshold
    number_above_max_latency = len(sk_dataframe[(sk_dataframe['whitebox']==target) & (sk_dataframe['latency'] > latency_threshold)])
    high_delay_df = sk_dataframe[(sk_dataframe['whitebox']==target) & (sk_dataframe['latency'] > latency_threshold)]

    if number_above_max_latency > 1:
        fail_rate_delay = int(float(number_above_max_latency)/float(total_entries)*100)
        print("whitebox: {} has {} below latency threshold of {} ms out of {} entries.  Fail Rate: {}".format(target,number_above_max_latency,latency_threshold,total_entries,fail_rate_delay))
        access_data.loc[(access_data['unitID']==int(target)),'failrateLatency']= fail_rate_delay
        print(high_delay_df)
    else:
        if total_entries > 0:
            fail_rate_delay = int(float(number_above_max_latency)/float(total_entries)*100)
            access_data.loc[(access_data['unitID']==int(target)),'failrateLatency']= fail_rate_delay
        else:
            access_data.loc[(access_data['unitID']==int(target)),'failrateLatency']= -1





print(access_data)


# Write to database


number_of_access = len(access_data)

c,conn=connection()

#clear the table

x = "delete from htdata;"
c.execute(x)
conn.commit()

for i in range(0,number_of_access):

    x = "insert into htdata (account,serial,unitID,accessID,coreID,accessType,failrateDownload,failrateUpload,failrateLatency)\
            values (\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",{},{},{});".format(str(access_data.iloc[i]['account']),str(access_data.iloc[i]['serial']),str(access_data.iloc[i]['unitID']),access_data.iloc[i]['accessID'],access_data.iloc[i]['coreID'],access_data.iloc[i]['accessType'],access_data.iloc[i]['failrateDownload'],access_data.iloc[i]['failrateUpload'],access_data.iloc[i]['failrateLatency'])


    
    c.execute(x)
    conn.commit()


conn.close()


