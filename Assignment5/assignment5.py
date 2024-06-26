#!/usr/bin/env python3

"""Script that extracts information about an organisms' features in a GBFF file.

It creates a DataFrame with all the features, containing their key, location, the
identifier and organism they belong to and which index the feature is at for its record.

Using the dataframe it answers a couple of questions about the file's features:
  1. How many features does an Archaea genome have on average?
  2. What is the proportion between coding and non-coding features?
  3. What are the minimum and maximum amount of proteins of all organisms in the file?
  4. What is the average length of a feature?

Lastly it saves a version of the DataFrame in Spark format which has all the non-coding
features removed from it.
"""

# METADATA
__author__ = "Vincent Talen"
__version__ = "0.1"

# IMPORTS
from pathlib import Path

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import explode, split


# FUNCTIONS
def extract_record_info(record: Row) -> Row:
    record_str: str = record.value
    new_row: dict = {}
    record_features: list[list[str]] = []
    at_features: bool = False
    for line in record_str.split("\n"):
        if at_features:
            if not line.startswith("     "):
                break
            if line[5] != " ":
                record_features.append(line.strip().split())
            continue

        if line.startswith("LOCUS"):
            new_row["identifier"] = line.split()[1]
        elif line.startswith("SOURCE"):
            new_row["organism"] = line.strip().split(maxsplit=1)[1]
        elif line.startswith("FEATURES"):
            at_features = True

    new_row["features"] = "//".join(
        [f"{i}, {f[0]}, {f[1]}" for i, f in enumerate(record_features, start=1)]
    )
    return Row(**new_row)


def split_feature_column(row: Row) -> Row:
    feature_str: str = row.features
    feature_info: list[str] = feature_str.split(", ")
    return Row(
        identifier=row.identifier,
        organism=row.organism,
        feature_index=int(feature_info[0]),
        feature_name=feature_info[1],
        feature_location=feature_info[2]
    )


def create_features_dataframe(spark: SparkSession, file: str) -> DataFrame:
    return (
        # Load the file as a DataFrame where each record is a row
        spark.read.text(file, lineSep="//\n")
        # Extract the useful information from each record
        .rdd.map(extract_record_info).toDF()
        # Explode the features in the "features" column into their own separate rows
        .withColumn("features", explode(split("features", "//")))
        # Split the information of features into separate columns
        .rdd.map(split_feature_column).toDF()
    )


# MAIN
def main():
    # Create SparkSession
    spark: SparkSession = (
        SparkSession.builder
        .master("local[16]")
        .appName("bdc_a5")
        .getOrCreate()
    )

    # Specify the directory to the data and which file to use
    data_dir = Path("/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release")
    # file = str(data_dir / "archaea" / "archaea.1.genomic.gbff")  # Real, large file
    file = str(data_dir / "archaea" / "archaea.2.rna.gbff")  # Tiny 'test' file

    # Create a DataFrame with all the features in the .gbff file
    features_df: DataFrame = create_features_dataframe(spark, file)
    features_df.show()


if __name__ == "__main__":
    main()
