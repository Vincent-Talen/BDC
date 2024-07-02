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
__version__ = "0.3"

# IMPORTS
from pathlib import Path

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col, explode, split

# GLOBALS
CODING_KEYS = ["CDS"]
NON_CODING_KEYS = ["ncRNA", "rRNA"]
ALL_DESIRED_KEYS = ["gene", *CODING_KEYS, *NON_CODING_KEYS]


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
        key=feature_info[1],
        location=feature_info[2]
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
        # Filter features by the desired keys and supported location format
        .filter(
            col("key").isin(ALL_DESIRED_KEYS)
            & col("location").rlike(r"^(?:complement\()?\d+\.{2}\d+\)?$")
        )
    )


def question2(features_df: DataFrame) -> float:
    # Q2: What is the proportion between coding and non-coding features?
    coding_features = features_df.filter(features_df.key.isin(CODING_KEYS))
    non_coding_features = features_df.filter(features_df.key.isin(NON_CODING_KEYS))
    return coding_features.count() / non_coding_features.count()


def question3(features_df: DataFrame) -> tuple[int, int]:
    # Q3: What are the min and max amount of proteins of all organisms in the file?
    protein_counts = (
        features_df
        .filter(features_df.key.isin(CODING_KEYS))
        .groupBy("organism")
        .agg({"key": "count"})
        .select("count(key)")
        .sort("count(key)")
        .collect()
    )
    return protein_counts[0][0], protein_counts[-1][0]


def answer_questions(features_df: DataFrame) -> None:
    # Q1: How many features does an Archaea genome have on average?
    q1 = features_df.groupBy("identifier").count().agg({"count": "mean"}).collect()[0][0]
    print(f"Answer 1: An Archaea genome has {q1:.2f} features on average.")

    # Q2: What is the proportion between coding and non-coding features?
    q2 = question2(features_df)
    print(
        f"Answer 2: "
        f"The proportion between coding and non-coding features is {q2:.2f} to 1."
    )

    # Q3: What are the min and max amount of proteins of all organisms in the file?
    q3_min, q3_max = question3(features_df)
    print(
        f"Answer 3: "
        f"The minimum amount of proteins in any organism is {q3_min} and "
        f"the maximum is {q3_max}."
    )

    # Q4: What is the average length of a feature?


# MAIN
def main():
    """Main function of the script."""
    # Create SparkSession
    spark: SparkSession = (
        SparkSession.builder
        .master("local[16]")
        .config("spark.executor.memory", "64g")
        .config("spark.driver.memory", "64g")
        # .config("spark.log.level", "ERROR")
        .appName("bdc_a5")
        .getOrCreate()
    )

    # Specify the directory to the data and which file to use
    data_dir = Path("/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release")
    file = str(data_dir / "archaea" / "archaea.1.genomic.gbff")
    # file = str(data_dir / "archaea" / "archaea.2.genomic.gbff")
    # file = str(data_dir / "archaea" / "archaea.3.genomic.gbff")
    print(f"\nPerforming analysis and answering questions for the following file:")
    print(f"  {file}\n")

    # Create a DataFrame with a filtered subset of features from the .gbff file
    features_df: DataFrame = create_features_dataframe(spark, file)

    # Answer the questions about the features
    answer_questions(features_df)

    # Save the DataFrame in Spark format with only coding features
    pass


if __name__ == "__main__":
    main()
