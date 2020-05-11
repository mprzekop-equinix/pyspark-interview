import unittest
import pyspark
import warnings
from pyspark.sql import SparkSession


class PySparkTestBase(unittest.TestCase):

    sparkSession = None

    @classmethod
    def setUpClass(cls):
        conf = pyspark.SparkConf().setMaster("local[2]").setAppName("interview testing")
        cls.sparkSession = SparkSession.builder.config(conf=conf).getOrCreate()
        cls.sparkContext = cls.sparkSession.sparkContext

    @classmethod
    def tearDownClass(cls):
        cls.sparkSession.stop()

    def setUp(self):
        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        warnings.simplefilter("default", ResourceWarning)
