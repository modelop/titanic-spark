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

#modelop.score
def score(input_file_loc, output_file_loc):
    df = (spark.read
          .format("csv")
          .option('header', 'true')
          .load(input_file_loc)

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

    model = RandomForestClassificationModel.load('/hadoop/titanic')

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
    predictions.write.csv(output_file_loc)
    return 

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("Titanic")\
        .getOrCreate()

    score('/hadoop/test.csv', '/hadoop/titanic_output.csv')
    spark.stop()
