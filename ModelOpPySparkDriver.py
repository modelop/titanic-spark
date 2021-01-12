import titanic_assets_access as modelop_model

if __name__ == "__main__":
    print("ModelOpPySparkExecution execution")
    #input
    #/hadoop/test.csv"
    #model
    #/hadoop/titanic
    #output
    #/hadoop/titanic_output.csv

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

    ## Executing requested function
    modelop_model.score(job_input, job_output)

    print("PySpark code execution completed")