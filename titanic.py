from __future__ import print_function

import sys
from random import random
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import isnull, when, count
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, FloatType
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# modelop.init
def init():
    print("Begin function...")

    global SPARK
    SPARK = SparkSession.builder.appName("DriftTest").getOrCreate()

    global MODEL
    MODEL = RandomForestClassificationModel.load("/hadoop/demo/titanic-spark/titanic")


# modelop.score
def score(external_inputs, external_outputs, external_model_assets):
    # Grab single input asset and single output asset file paths
    input_asset_path, output_asset_path = parse_assets(
        external_inputs, external_outputs
    )

    input_df = SPARK.read.format("csv").option("header", "true").load(input_asset_path)
    predictions = predict(input_df)

    predictions.limit(5).show()
    predictions = predictions.select(
        "Pclass", "Age", "Gender", "Fare", "Boarded", "prediction"
    )

    predictions.limit(5).show()
    # Use coalesce() so that the output CSV is a single file for easy reading
    predictions.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        output_asset_path
    )

    SPARK.stop()


# modelop.metrics
def metrics(external_inputs, external_outputs, external_model_assets):
    # Grab single input asset and single output asset file paths
    input_asset_path, output_asset_path = parse_assets(
        external_inputs, external_outputs
    )

    input_df = SPARK.read.format("csv").option("header", "true").load(input_asset_path)
    predictions = predict(input_df)

    # Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(
        labelCol="Survived", predictionCol="prediction", metricName="accuracy"
    )
    accuracy = evaluator.evaluate(predictions)

    output_df = SPARK.createDataFrame([{"accuracy": accuracy}])
    print("Metrics output:")
    output_df.show()

    output_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        output_asset_path
    )

    SPARK.stop()


def predict(input_df):
    dataset = input_df.select(
        col("Survived").cast("float"),
        col("Pclass").cast("float"),
        col("Sex"),
        col("Age").cast("float"),
        col("Fare").cast("float"),
        col("Embarked"),
    )

    dataset = dataset.replace("?", None).dropna(how="any")

    dataset = (
        StringIndexer(inputCol="Sex", outputCol="Gender", handleInvalid="keep")
        .fit(dataset)
        .transform(dataset)
    )

    dataset = (
        StringIndexer(inputCol="Embarked", outputCol="Boarded", handleInvalid="keep")
        .fit(dataset)
        .transform(dataset)
    )

    dataset = dataset.drop("Sex")
    dataset = dataset.drop("Embarked")

    required_features = ["Pclass", "Age", "Fare", "Gender", "Boarded"]

    assembler = VectorAssembler(inputCols=required_features, outputCol="features")
    transformed_data = assembler.transform(dataset)

    predictions = MODEL.transform(transformed_data)
    return predictions


def parse_assets(external_inputs, external_outputs):
    """Returns a tuple (input asset hdfs path, output asset hdfs path)"""

    # Fail if more assets than expected
    if len(external_inputs) != 1:
        raise ValueError("Only one input asset should be provided")
    if len(external_outputs) != 1:
        raise ValueError("Only one output asset should be provided")

    # There's only one key-value pair in each dict, so
    # grab the first value from both
    input_asset = list(external_inputs.values())[0]
    output_asset = list(external_outputs.values())[0]

    # Fail if assets are JSON
    if ("fileFormat" in input_asset) and (input_asset["fileFormat"] == "JSON"):
        raise ValueError("Input file format is set as JSON but must be CSV")
    if ("fileFormat" in output_asset) and (output_asset["fileFormat"] == "JSON"):
        raise ValueError("Output file format is set as JSON but must be CSV")

    # Return paths from file URLs
    input_asset_path = input_asset["fileUrl"]
    output_asset_path = output_asset["fileUrl"]

    return (input_asset_path, output_asset_path)
