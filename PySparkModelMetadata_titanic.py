job_input = {
         "test.csv":{
             "assetType": "EXTERNAL_FILE",
             "fileUrl": "/hadoop/test.csv",
             "filename": "test.csv",
             "fileFormat": "CSV"
         },
         "titanic":{
             "assetType": "EXTERNAL_FILE",
             "fileUrl": "/hadoop/titanic",
             "filename": "titanic"
         }
         }

job_output = {
    "titanic_output.csv":{
       "assetType": "EXTERNAL_FILE",
       "fileUrl": "/hadoop/titanic_output.csv",
       "filename": "titanic_output.csv",
       "fileFormat": "CSV"
        }
    }