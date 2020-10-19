from pyspark.ml.evaluation import RegressionEvaluator


def performance(model, test_data):
    predictions = model.transform(test_data)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    RMSE = evaluator.evaluate(predictions)
    return RMSE;
