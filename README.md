# spark-example

To run this example:
* Follow the Spark container setup here: https://modelop.atlassian.net/wiki/spaces/VDP/pages/1283293189/Spark+Integration
* Unzip titanic.zip
* Run `scp -r LAPTOP_USERNAME@host.docker.internal:/Users/LAPTOP_USERNAME/path/to/repo/titanic/ .` from the container.
* Run `scp LAPTOP_USERNAME@host.docker.internal:/Users/LAPTOP_USERNAME/path/to/repo/test.csv` also from the container.
* Run `hadoop fs -mkdir -p /hadoop/` to specify the path in hdfs also from the container.
* Run `hadoop fs -put titanic/ /hadoop/titanic/` to put the model object in hdfs.
* Run `hadoop fs -put test.csv /hadoop/test.csv` so the test data is available.
* Run `./spark-submit --master yarn --executor-memory 512MB --total-executor-cores 10 /Users/LAPTOP_USERNAME/path/to/repo/titanic.py`
* There should be an hdfs CSV file name titanic_output.csv in the container. (`hadoop fs -ls /hadoop/` should list all files)
