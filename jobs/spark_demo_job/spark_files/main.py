import pyspark

sc = pyspark.SparkContext()
rdd = sc.parallelize(['Hello,', 'world!', 'to', 'Fynd', 'Ignite'])
words = sorted(rdd.collect())
print(words)
