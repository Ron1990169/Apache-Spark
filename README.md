This document is an academic report submitted by Rohin Mehra for Assignment 2 as part of the MSc. Big Data Analysis and Management program at Griffith College Dublin. 
The assignment focuses on analyzing CRAN package download logs using Apache Spark, specifically leveraging the RDD API for processing large datasets. 
The report highlights the approach, tools, and methodologies used to extract insights into R package downloads, popularity, and usage patterns.

Abstract
This analysis employs the CRAN package download logs dataset to answer various questions regarding R package downloads, popularity, and usage patterns. 
The dataset includes fields such as the download date, time, package size, R version, architecture, operating system, package name, version, country, and a unique IP identifier. 
Apache Spark, a powerful big data processing framework, is utilized to process the data and perform various analytics tasks using the RDD API. Additionally, 
two Python libraries—pyspark.sql and datetime—are used to facilitate the data processing and analysis.

Dataset Fields
The dataset consists of the following fields:

date: Date of download
time: Time of download
size: Size of the downloaded package (in bytes)
r_version: Version of R used for the download
r_arch: Processor architecture (e.g., i386 for 32-bit, x86_64 for 64-bit)
r_os: Operating System (e.g., macOS, Windows)
package: Name of the downloaded package
version: Version of the downloaded package
country: Two-letter ISO country code
ip_id: Unique daily identifier for each IP address
Tools and Technologies
Apache Spark
Apache Spark is a distributed data processing framework that allows for the handling of large-scale data across clusters of computers. 
It provides APIs for working with structured and semi-structured data, supporting operations through Resilient Distributed Datasets (RDDs) and DataFrames.

Python Libraries:
pyspark.sql: A library within Apache Spark that facilitates working with structured data using DataFrame and SQL-like queries. It is crucial for performing data analysis 
and transformations on large datasets.
datetime: Part of the Python Standard Library, this module provides classes and methods for manipulating dates and times, including support for time zones.
It is used to handle date and time information in the dataset.

Methodology:
Data Processing with Apache Spark
The analysis begins by loading the CRAN package download logs into an RDD in Apache Spark. The RDD API is then used to perform various 
transformations and actions to analyze the data. The steps include filtering, mapping, reducing, and grouping the data to extract meaningful insights.

Key Analysis Tasks:
The following key tasks were performed using Apache Spark:

Count of Package Downloads: The total number of downloads for each package was calculated to determine popularity.
Country-based Analysis: The data was grouped by the country field to analyze download patterns across different regions.
Version and OS Analysis: The data was examined to identify the most commonly used versions of R and operating systems among users.
Time-based Analysis: The datetime library was used to parse and manipulate date and time fields to analyze download trends over time.
Results
1. Package Popularity
The analysis identified the most downloaded R packages, with particular attention to widely used packages like ggplot2, dplyr, and shiny.
The results showed significant differences in download counts, reflecting the varying levels of popularity among R users.

3. Regional Download Patterns
By grouping downloads by country, the analysis highlighted the regions with the highest R usage. The United States, China, and Germany were among the top countries with
the most downloads, indicating strong R communities in these regions.

5. R Version and OS Preferences
The analysis of R versions and operating systems revealed that most users were using the latest versions of R and predominantly operating on 64-bit architectures.
The mingw32 (Windows) and darwin (macOS) operating systems were the most common among the users.

7. Temporal Trends in Downloads
Temporal analysis showed trends in package downloads over different times of the day and across different dates. Peak download times were identified, providing
insights into when users are most active.

Conclusion
The assignment successfully utilized Apache Spark to analyze the CRAN package download logs dataset, uncovering key insights into R package popularity, regional 
usage patterns, and user preferences for R versions and operating systems. The use of pyspark.sql and datetime in conjunction with the RDD API demonstrated the powerful 
capabilities of Apache Spark in processing and analyzing large datasets efficiently. The findings provide valuable information that can be used to understand and improve 
the distribution and usage of R packages across the global R community.
