########################################################################################
##### dynamic model / resources content.
########################################################################################

# primary source code
import model_source_code as modelop_model_source_code
# job metadata, variables pointing to [input, output, appname,etc]
import modelop_job_metadata as modelop_job_metadata


########################################################################################
##### static resources content.
########################################################################################

if __name__ == "__main__":
    print("### - ModelOpPySparkExecution execution")

    ## Executing requested function
    print("### - Executing :" + modelop_job_metadata.job_function)
    getattr(modelop_model, modelop_job_metadata.job_function)(modelop_job_metadata.job_input, modelop_job_metadata.job_output)
    print("### - PySpark code execution completed")