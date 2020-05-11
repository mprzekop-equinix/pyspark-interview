from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.getOrCreate()
    sensor_df = import_sensors_file(spark)
    sensor_df.show()

    spark.stop()


def import_sensors_file(spark):
    df = spark.read.format('csv') \
        .options(header='true', inferSchema='true', delimiter=';') \
        .load('test/test_data/sensors.csv')

    return df


if __name__ == '__main__':
    main()
