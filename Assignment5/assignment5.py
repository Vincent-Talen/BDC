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
__version__ = "0.5"

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


def split_location_column(row: Row) -> Row:
    location_str: str = row.location
    complement = False
    if location_str.startswith("complement"):
        complement = True
        location_str = location_str[11:-1]
    start, stop = location_str.split("..")
    return Row(
        identifier=row.identifier,
        organism=row.organism,
        feature_index=row.feature_index,
        key=row.key,
        start=int(start),
        stop=int(stop),
        complement=complement,
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
        # Split the location column into separate columns
        .rdd.map(split_location_column).toDF()
    )


def remove_coding_gene_features(features_df: DataFrame) -> DataFrame:
    all_genes = features_df.filter(col("key").like("gene")).alias("gene")
    all_cds = features_df.filter(col("key").like("CDS")).alias("cds")

    join_condition = [
        col("gene.identifier") == col("cds.identifier"),
        col("gene.start") <= col("cds.start"),
        col("gene.stop") >= col("cds.stop"),
        col("gene.complement") == col("cds.complement"),
    ]
    only_non_coding_genes = all_genes.join(all_cds, on=join_condition, how="left_anti")
    return features_df.filter(~col("key").like("gene")).union(only_non_coding_genes)


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


def question4(features_df: DataFrame) -> float:
    # Q4: What is the average length of a feature?
    return (
        features_df
        .withColumn("length", col("stop") - col("start"))
        .agg({"length": "mean"})
        .first()[0]
    )


def answer_questions(features_df: DataFrame) -> None:
    # Q1: How many features does an Archaea genome have on average?
    q1 = features_df.groupBy("identifier").count().agg({"count": "mean"}).first()[0]
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
    q4 = question4(features_df)
    print(f"Answer 4: The average length of a feature is {q4:.2f}.")


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
    print(f"\nPerforming analysis and answering questions for the following file:")
    print(f"  {file}\n")
    file = data_dir / "archaea" / "archaea.1.genomic.gbff"
    # file = data_dir / "archaea" / "archaea.2.genomic.gbff"
    # file = data_dir / "archaea" / "archaea.3.genomic.gbff"

    # Create a DataFrame with a filtered subset of features from the .gbff file
    features_df: DataFrame = create_features_dataframe(spark, str(file))
    features_df = remove_coding_gene_features(features_df)

    # Answer the questions about the features
    answer_questions(features_df)

    # Save the DataFrame in Spark format with only coding features
    output_file = file.stem + "_coding_features.parquet"
    features_df.filter(col("key").isin(CODING_KEYS)).write.parquet(output_file)


if __name__ == "__main__":
    main()
