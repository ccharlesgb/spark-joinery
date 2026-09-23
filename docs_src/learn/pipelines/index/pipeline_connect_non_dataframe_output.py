from dataclasses import dataclass
from typing import Annotated

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import VectorUDT
from pyspark.ml.regression import LinearRegression, LinearRegressionModel
from pyspark.sql import DataFrame, SparkSession

from spark_joinery import Pipeline, Strict, transform


@dataclass
class Housing:
    number_of_bedrooms: int
    square_footage: int
    price: float


@dataclass
class PreparedHousing:
    number_of_bedrooms: int
    square_footage: int
    price: float
    features: Annotated[object, VectorUDT()]


@transform
def read_data(spark: SparkSession) -> Annotated[DataFrame, Strict(Housing)]:
    return spark.createDataFrame(
        [
            Housing(1, 650, 180_000.0),
            Housing(2, 900, 260_000.0),
            Housing(3, 1_200, 360_000.0),
            Housing(4, 1_600, 480_000.0),
        ]
    )


@transform
def prepare_features(
    housing: Annotated[DataFrame, Strict(Housing)],
) -> Annotated[DataFrame, Strict(PreparedHousing)]:
    return VectorAssembler(
        inputCols=["number_of_bedrooms", "square_footage"],
        outputCol="features",
    ).transform(housing)


@transform
def fit_model(
    housing: Annotated[DataFrame, Strict(PreparedHousing)],
) -> LinearRegressionModel:
    return LinearRegression(featuresCol="features", labelCol="price").fit(housing)


@transform
def print_coefficients(model: LinearRegressionModel) -> None:
    print(f"coefficients: {model.coefficients}")
    print(f"intercept: {model.intercept}")


housing_price_model = Pipeline()
read_data_step = housing_price_model.add_step(read_data)
prepare_features_step = housing_price_model.add_step(prepare_features)
fit_model_step = housing_price_model.add_step(fit_model)
print_coefficients_step = housing_price_model.add_step(print_coefficients)

housing_price_model.connect(read_data_step, prepare_features_step)
housing_price_model.connect(prepare_features_step, fit_model_step)
housing_price_model.connect(fit_model_step, print_coefficients_step)

housing_price_model.run(spark=SparkSession.builder.getOrCreate())
