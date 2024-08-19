from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime

# Stop SparkContext if it already exists
try:
    sc.stop()
except NameError:
    pass

sc = SparkContext("local", "RDD Analytics")
spark = SparkSession(sc)

downloadsRDD = sc.textFile("hdfs://localhost:9000/input/2023-03-01.csv")


# Question 1
def get_ggplot2_downloads(record):
    fields = record.split(',')
    package_name = fields[6]
    return package_name == 'ggplot2'


ggplot2_downloads = downloadsRDD.filter(get_ggplot2_downloads).count()
print(f"Number of downloads for package ggplot2: {ggplot2_downloads}")


# Question 2
def get_country_downloads(record):
    fields = record.split(',')
    country = fields[3]
    return country, 1


country_downloads = downloadsRDD.map(get_country_downloads)
country_downloads_count = country_downloads.reduceByKey(lambda a, b: a + b)
max_downloads_country = country_downloads_count.sortBy(lambda x: x[1], ascending=False).first()

print(f"Highest number of downloads by a country: {max_downloads_country[0]} with {max_downloads_country[1]} downloads")


# Question 3
def get_package_downloads(record):
    fields = record.split(',')
    package_name = fields[6]
    return (package_name, 1)


package_counts = downloadsRDD.map(get_package_downloads).reduceByKey(lambda a, b: a + b)
top_10_packages = package_counts.sortBy(lambda x: x[1], ascending=False).take(10)

print("Top 10 most popular packages:")
for package, count in top_10_packages:
    print(f"{package}: {count}")


# Question 4
def get_irish_downloads(record):
    fields = record.split(',')
    country = fields[3]
    package_name = fields[6]
    if country == "IE":
        return (package_name, 1)
    else:
        return (None, 0)


irish_downloads = downloadsRDD.map(get_irish_downloads).reduceByKey(lambda a, b: a + b)
most_popular_irish_package = irish_downloads.sortBy(lambda x: x[1], ascending=False).first()

print(f"Most popular package in Ireland: {most_popular_irish_package[0]}")


# Question 5
def get_os(record):
    fields = record.split(',')
    os = fields[5]
    return (os, 1)


os_counts = downloadsRDD.map(get_os).reduceByKey(lambda a, b: a + b)
most_popular_os = os_counts.sortBy(lambda x: x[1], ascending=False).first()

print(f"Most popular OS among R programmers: {most_popular_os[0]}")


# Question 6
def group_similar_os(os):
    if "windows" in os.lower():
        return "Windows"
    elif "linux" in os.lower():
        return "Linux"
    elif "mac" in os.lower():
        return "Mac"
    else:
        return "Other"


grouped_os_counts = os_counts.map(lambda x: (group_similar_os(x[0]), x[1])).reduceByKey(lambda a, b: a + b)
print("Total number of downloads by each Operating System:")
for os, count in grouped_os_counts.collect():
    print(f"{os}: {count}")


# Question 7
def parse_size_record(record):
    fields = record.split(',')
    package_name = fields[6]
    package_size = float(fields[8])
    return (package_name, package_size)


distinct_package_sizes = downloadsRDD.map(parse_size_record).distinct()
top_10_largest_packages = distinct_package_sizes.sortBy(lambda x: x[1], ascending=False).take(10)

print("Top 10 (distinct) largest-sized packages:")
for package, size in top_10_largest_packages:
    print(f"{package}: {size} MB")

# Question 8


def parse_record(record):
    fields = record.split(',')
    timestamp_str = fields[1]
    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
    hour = timestamp.hour
    return (hour, 1)


hours_counts = downloadsRDD.map(parse_record).reduceByKey(lambda a, b: a + b)
sorted_hours_counts = hours_counts.sortBy(lambda x: x[1], ascending=False)
top_3_hours = sorted_hours_counts.take(3)

print("Top 3 hours with respect to the number of download hits:")
for hr, count in top_3_hours:
    print(f"{hr}: {count}")


# Question 9
def get_us_downloads(record):
    fields = record.split(',')
    country = fields[3]
    package_name = fields[6]
    if country == "US":
        return (package_name, 1)
    else:
        return (None, 0)


us_downloads = downloadsRDD.map(get_us_downloads).reduceByKey(lambda a, b: a + b)
top_5_us_packages = us_downloads.sortBy(lambda x: x[1], ascending=False).take(5)

print("Top 5 most popular packages in the US:")
for package, count in top_5_us_packages:
    print(f"{package}: {count}")


# Question 10
def count_32_bit_users(record):
    fields = record.split(',')
    arch = fields[4]
    return (1 if "i386" in arch else 0)


num_32_bit_users = downloadsRDD.map(count_32_bit_users).reduce(lambda a, b: a + b)

print(f"Number of R users still using 32-bit machines: {num_32_bit_users}")
