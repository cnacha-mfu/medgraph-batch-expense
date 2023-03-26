import joblib
import os
import pandas as pd
import json
import firebase_admin
from google.cloud import storage
from firebase_admin import credentials
from firebase_admin import firestore
import requests
 
def tranform_bmi(x):
    if x < 18.5 :
        return 1
    elif 18.5 <= x <= 24.9 :
        return 2
    elif 25 <= x <= 29.9 :
        return 3
    elif 30 <= x <= 34.9 :
        return 4
    elif 35 <= x <= 39.9 :
        return 5
    else:
        return 6   

def combine_expln(pos_dict,neg_dict, pdict):
    for key in pdict:
        if pdict[key] > 0:
            if key in pos_dict: 
                pos_dict[key] = pos_dict[key] + pdict[key]
            else:
                pos_dict[key] = pdict[key]
        else:
            if key in neg_dict: 
                neg_dict[key] = neg_dict[key] + pdict[key]
            else:
                neg_dict[key] = pdict[key]
       
    return pos_dict, neg_dict
    
api_endpoint_url = os.environ['URI_SERVICE_ENDPOINT']

cred = credentials.Certificate("service-account.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

import datetime 
now = datetime.datetime.now()


#update status of all summary documents
docs = db.collection(u'expense_summary').where(u'status', u'==', 1).stream()
for doc in docs:
    doc.reference.update({"status": 0})

## intitalise the summary record for each level
docs = db.collection(u'department').stream()
for dep_doc in docs:
     dep_data = dep_doc.to_dict()
     for i in range(4):
        summary_row = { "update_date": firestore.SERVER_TIMESTAMP,
                       "update_date_label": now.strftime('%d %b'),
                       "level": i+1,
                       "count": 0,
                       "status": 1,
                       "department": dep_data['name']}
        print("initialise level summary: "+str(i+1)+" for "+dep_data['name'])
        #db.collection(u'expense_summary').document(dep_data['name']+str(i+1)).set(summary_row)
        update_time, doc_ref = db.collection(u'expense_summary').add(summary_row)

## make prediction for each person 
docs = db.collection(u'person').stream()
import json

pos_dict = {}
neg_dict = {}
for doc in docs:
    item = doc.to_dict()
    #print(item['health_record'].path)
    record_ref = db.document(item['health_ref'].path)
    record = record_ref.get().to_dict()
    #print(record)
    reqest_data = { "instances": [{
                "age":record['Age'],
                "sex":record['Sex'],
                "bmi":tranform_bmi(record['BMI']),
                "steps": record['Steps'],
                "children": record['Children'],
                "smoker": record['Smoker'],
                }] }
   
    response = requests.post(api_endpoint_url, json=reqest_data)
    result = json.loads(response.text)
    print(result['predictions']['results'][0], doc.id)
    print(result['predictions']['explainations'][0])

    prediction_data = {'output': result['predictions']['results'][0]}
    expln = result['predictions']['explainations'][0]
    prediction_data.update(expln)
    prediction_data['timestamp'] = firestore.SERVER_TIMESTAMP
    prediction_data['health_ref'] = record_ref
        
    db.collection(u'prediction_expense').document(record_ref.id).set(prediction_data)
    
    #combine explaination summary with current one
    pos_dict, neg_dict  = combine_expln(pos_dict,neg_dict, expln)
    
    # if(result['predictions']['results'][0] == 1):
    #     level_count[0] = level_count[0]+1
    # elif(result['predictions']['results'][0] == 2):
    #     level_count[1] = level_count[1]+1
    # elif(result['predictions']['results'][0] == 3):
    #     level_count[2] = level_count[2]+1
    # elif(result['predictions']['results'][0] == 4):
    #     level_count[3] = level_count[3]+1

    ## record summary for level
    level = result['predictions']['results'][0]
    summary_docs = db.collection(u'expense_summary').where(u'department', u'==', item['department']).where(u'level', u'==', level).order_by(u'update_date', direction=firestore.Query.DESCENDING).limit(1).stream()
    for summary_doc in summary_docs:
        print("summarise for level: "+str(level))
        summary_doc.reference.update({"count": firestore.Increment(1)})

#record explaination summary
for factor in pos_dict:
    factor_item = {'name':factor,'value':pos_dict[factor],'type':1}
    db.collection(u'expln_summary_expense').document("pos"+factor).set(factor_item)

for factor in neg_dict:
    factor_item = {'name':factor,'value':neg_dict[factor],'type':-1}
    db.collection(u'expln_summary_expense').document("neg"+factor).set(factor_item)
        
## overal summary for the whole company
# docs = db.collection(u'department').stream()
# for dep_doc in docs:
#     dep_data = dep_doc.to_dict()
#     summary_docs = db.collection(u'expense_summary').where(u'department', u'==', dep_data['name']).order_by(u'update_date', direction=firestore.Query.DESCENDING).order_by(u'level', direction=firestore.Query.ASCENDING).limit(4).stream()
#     overall_expense = {'department':dep_data['name'], 'update_date':firestore.SERVER_TIMESTAMP}
#     for summary_doc in summary_docs:
#             summary_dict = summary_doc.to_dict()
#             overall_expense['level'+str(summary_dict['level'])] = summary_dict['count']
    
#     db.collection(u'expense_overall').add(overall_expense)
    
    
