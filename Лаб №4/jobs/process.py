from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, month, to_date, desc
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Ініціалізація Spark
spark = SparkSession.builder.appName("TripsAnalysis").getOrCreate()

# Читання CSV
df = spark.read.csv("/opt/spark/jobs/trips.csv", header=True, inferSchema=True)

# Додавання колонки з датою
df = df.withColumn("start_date", to_date(col("start_time")))

# a) середня тривалість поїздки на день
def avg_trip_duration_per_day():
    result = df.groupBy("start_date").agg(avg("tripduration").alias("avg_duration"))
    result.write.mode("overwrite").csv("/opt/spark/out/avg_duration_per_day", header=True)

# b) кількість поїздок кожного дня
def trips_per_day():
    result = df.groupBy("start_date").agg(count("*").alias("trip_count"))
    result.write.mode("overwrite").csv("/opt/spark/out/trips_per_day", header=True)

# c) найпопулярніша початкова станція для кожного місяця
def popular_station_per_month():
    temp = df.withColumn("month", month("start_date")) \
        .groupBy("month", "from_station_name") \
        .agg(count("*").alias("trips"))

    window = Window.partitionBy("month").orderBy(desc("trips"))

    result = temp.withColumn("rank", row_number().over(window)) \
        .filter(col("rank") == 1)

    result.write.mode("overwrite").csv("/opt/spark/out/popular_station_per_month", header=True)

# d) топ 3 станції кожного дня
def top3_stations_per_day():
    temp = df.groupBy("start_date", "from_station_name") \
        .agg(count("*").alias("trips"))

    window = Window.partitionBy("start_date").orderBy(desc("trips"))

    result = temp.withColumn("rank", row_number().over(window)) \
        .filter(col("rank") <= 3)

    result.write.mode("overwrite").csv("/opt/spark/out/top3_stations_per_day", header=True)

# e) хто їздить довше
def avg_duration_by_gender():
    result = df.groupBy("gender").agg(avg("tripduration").alias("avg_duration"))
    result.write.mode("overwrite").csv("/opt/spark/out/avg_duration_by_gender", header=True)

# Виклик функцій
avg_trip_duration_per_day()
trips_per_day()
popular_station_per_month()
top3_stations_per_day()
avg_duration_by_gender()

spark.stop()