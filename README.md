# titanic-spark

This repo is an example Spark model that is conformed for use with ModelOp Center and the ModelOp Spark Runtime Service.

## Assets

There are three assets that are used to run this example:

| Asset Type | Repo File | HDFS Path | Description |
| --- | --- | --- | --- |
| Model Binary | `titanic.zip` | `/hadoop/demo/titanic-spark/titanic` | Spark model binary compressed as a zip file in this repo, but must be expanded and be available in the Spark cluster HDFS for the model's `init()` function to run |
| Input Asset | `test.csv` | `/hadoop/demo/titanic-spark/test.csv` | Input file for the model `score()` function. The HDFS path can vary based on the `external_inputs` param of the `score()` function  |
| Output Asset | `titanic_output.csv` | `/hadoop/demo/titanic-spark/titanic_output.csv` | Output file from the model `score()` function. The HDFS path can vary based on the `external_outputs` param of the `score()` function  |

## Mocaasin Tests

It is recommended to use the image `modelop/spark-cluster:dev-bp-ds` for the Spark cluster during testing because it includes libraries and assets that are needed to run this model.

1. Verify that the model binary (above) exists at `/hadoop/demo/titanic-spark/titanic` in the Spark cluster HDFS and verify that the input asset exists at `/hadoop/demo/titanic-spark/test.csv`. The input asset can be in a different location, but you must update the input asset URL in step (3) below.
2. Import this repository to ModelOp Center
3. Create a new scoring job with the following HDFS URL assets (do not select the "Secured" option):
  - Input asset: `hdfs:///hadoop/demo/titanic-spark/test.csv`
  - Output asset: `hdfs:///hadoop/demo/titanic-spark/titanic_output.csv`
4. Wait for the job to enter the `COMPLETE` state
5. Inside the Spark cluter, use `hadoop fs -getmerge -nl /hadoop/demo/titanic-spark/titanic_output.csv /home/cloudera/titanic_output.csv` and `kubectl cp SPARK_CLUSTER_POD_NAME:/home/cloudera/titanic_output.csv titanic_output.csv` to copy the output asset to your local computer.
  - After running `hadoop fs -getmerge ...`, you can run `cat titanic_output.csv` in the Spark cluster and you should see the following printed to the terminal:

```
Pclass,Age,Gender,Fare,Boarded,prediction
3.0,26.0,0.0,7.8958,0.0,0.0
1.0,25.0,1.0,151.55,0.0,1.0
3.0,40.0,0.0,7.225,1.0,0.0
3.0,1.0,1.0,11.1333,0.0,1.0
3.0,11.0,0.0,46.9,0.0,0.0
2.0,36.0,1.0,26.0,0.0,1.0
...
```

## Local Tests

To run this example locally:
- Follow the Spark container setup here: https://modelop.atlassian.net/wiki/spaces/VDP/pages/1283293189/Spark+Integration
- Unzip titanic.zip
- Run `scp -r LAPTOP_USERNAME@host.docker.internal:/Users/LAPTOP_USERNAME/path/to/repo/titanic/ .` from the container.
- Run `scp LAPTOP_USERNAME@host.docker.internal:/Users/LAPTOP_USERNAME/path/to/repo/test.csv` also from the container.
- Run `hadoop fs -mkdir -p /hadoop/` to specify the path in hdfs also from the container.
- Run `hadoop fs -put titanic/ /hadoop/titanic/` to put the model object in hdfs.
- Run `hadoop fs -put test.csv /hadoop/test.csv` so the test data is available.
- Run `./spark-submit --master yarn --executor-memory 512MB --total-executor-cores 10 /Users/LAPTOP_USERNAME/path/to/repo/titanic.py`
- There should be an hdfs CSV file name titanic_output.csv in the container. (`hadoop fs -ls /hadoop/` should list all files)
