import unittest
import pyspark
import warnings
from pyspark.sql import SparkSession


class PySparkTestBase(unittest.TestCase):

    spark = None

    @classmethod
    def setUpClass(cls):
        conf = pyspark.SparkConf().setMaster("local[2]").setAppName("interview testing")
        cls.spark = SparkSession.builder.config(conf=conf).getOrCreate()
        cls.spark_ctx = cls.spark.sparkContext

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        warnings.simplefilter("default", ResourceWarning)
