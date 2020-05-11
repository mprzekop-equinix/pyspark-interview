from test.testutils.base_test import PySparkTestBase
from operator import add


class SimpleTestCase(PySparkTestBase):

    def test_with_rdd(self):
        test_input = [
            ' hello spark ',
            ' hello again spark spark'
        ]

        input_rdd = self.spark_ctx.parallelize(test_input, 1)

        results = input_rdd.flatMap(lambda text: text.split()).map(lambda word: (word, 1)).reduceByKey(add).collect()
        print(results)
        self.assertEqual(results, [('hello', 2), ('spark', 3), ('again', 1)])

    def test_with_df(self):
        df = self.spark.createDataFrame(data=[[1, 'a'], [2, 'b']],
                                        schema=['column1', 'column2'])
        df.show()
        self.assertEqual(df.count(), 2)




