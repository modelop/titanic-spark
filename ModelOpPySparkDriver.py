########################################################################################
##### dynamic model / resources content.
########################################################################################

# primary source code
import titanic_assets_access as modelop_model
# job metadata, variables pointing to [input, output, appname,etc]
import PySparkModelMetadata_titanic as modelop_job_metadata


########################################################################################
##### static resources content.
########################################################################################



if __name__ == "__main__":
    print("ModelOpPySparkExecution execution")


    ## Executing requested function
    modelop_model.score(modelop_job_metadata.job_input, modelop_job_metadata.job_output)

    print("PySpark code execution completed")