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

# modelop.score
def score(input_files, output_files):
    print("input_files contents: " + input_files)
    print("output_files contents: " + output_files)

    print("input fileUrl: " + input_files['test.csv']['fileUrl'])
    print("input fileUrl 2: " + input_files['titanic']['fileUrl'])
    print("output fileUrl: " + output_files['titanic_output.csv']['fileUrl'])

    df = (spark.read
          .format("csv")
          .option('header', 'true')
          .load(input_files['test.csv']['fileUrl']))

    dataset = df.select(col('Pclass').cast('float'),
                        col('Sex'),
                        col('Age').cast('float'),
                        col('Fare').cast('float'),
                        col('Embarked')
                        )

    dataset = dataset.replace('?', None)\
        .dropna(how='any')

    dataset = StringIndexer(
        inputCol='Sex',
        outputCol='Gender',
        handleInvalid='keep').fit(dataset).transform(dataset)

    dataset = StringIndexer(
        inputCol='Embarked',
        outputCol='Boarded',
        handleInvalid='keep').fit(dataset).transform(dataset)

    dataset = dataset.drop('Sex')
    dataset = dataset.drop('Embarked')

    required_features = ['Pclass',
                    'Age',
                    'Fare',
                    'Gender',
                    'Boarded'
                   ]

    assembler = VectorAssembler(inputCols=required_features, outputCol='features')
    transformed_data = assembler.transform(dataset)

    model = RandomForestClassificationModel.load(input_files['titanic']['fileUrl'])

    predictions = model.transform(transformed_data)
    get_propensity = udf(lambda x: x[1], ArrayType(FloatType()))
    print(predictions.head(5))
    predictions = predictions.select('Pclass',
                        'Age',
                        'Gender',
                        'Fare',
                        'Boarded',
                        'prediction'
                        )

    print(predictions.head(5))
    predictions.write.csv(output_files['titanic_output.csv']['fileUrl'])
    return

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("Titanic")\
        .getOrCreate()

    score(input_files['test.csv']['fileUrl'], output_files['titanic_output.csv']['fileUrl'])
    spark.stop()
