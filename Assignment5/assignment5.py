#!/usr/bin/env python3

"""Script that extracts information about an organisms' features in a GBFF file.

It creates a DataFrame with all the features, containing their key, location, the
identifier and organism they belong to and which index the feature is at for its record.

Using the dataframe it answers a couple of questions about the file's features:
  1. How many features does an Archaea genome have on average?
  2. What is the proportion between coding and non-coding features?
  3. What are the minimum and maximum amount of proteins of all organisms in the file?
  4. What is the average length of a feature?

Lastly it saves a version of the DataFrame in Spark (.parquet) format with only all the
coding features from the file.
"""

# METADATA
__author__ = "Vincent Talen"
__version__ = "0.6"

# IMPORTS
from pathlib import Path

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col, explode, split

# GLOBALS
CODING_KEYS = ["CDS"]
NON_CODING_KEYS = ["ncRNA", "rRNA", "gene"]
ALL_DESIRED_KEYS = [*CODING_KEYS, *NON_CODING_KEYS]


# FUNCTIONS
def extract_record_info(record: Row) -> Row:
    """Extracts record information from a single record in a GBFF file.

    It splits the lines of the record string on "\n" and parses through them, saving the
    record's identifier and source organism in their own columns. The features of the
    record are saved in a column as a single string formatted as "index, key, location",
    with "//" separating each feature.

    Args:
        record:
            A pyspark Row object with a column named "value" that contains the full
            genome/record in the shape of a single, long string.

    Returns:
        A new Row object for the records with the following columns:
            - identifier: The locus identifier of the record.
            - organism: The source organism of the record.
            - features: A string with the features of the record split by "//".
    """
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
    """Splits all the feature-specific info from a feature row into separate columns.

    Args:
        row:
            A pyspark Row object of a single feature with its feature-specific info
            saved as a single string, separated by ", ", under the "features" column.

    Returns:
        A new Row object still containing the "identifier" and "organism" columns but
        now also with the "features" column split into the following separate columns:
            - feature_index: The index of the feature for the record it is part of.
            - key: The key of the feature.
            - location: The location of the feature.
    """
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
    """Splits the location string of a feature into separate columns.

    Args:
        row:
            A pyspark Row object of a single feature with its location being a single
            string under the "location" column. The supported types of locations are
            those with certain start and stop positions, with or without a complement.

    Returns:
        A new Row object with the same columns as the input row but with the location
        column split into the following separate columns:
            - start: The start position of the feature.
            - stop: The stop position of the feature.
            - complement: A boolean indicating if the location is a complement.
    """
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
    """Create a DataFrame with the features from a .gbff file.

    It loads the records of the GBFF file separately using the // separator and extracts
    the locus identifier, source organism and the features of each record. It then
    splits the features saved as a single string into their own separate rows, whilst
    also splitting the location info into multiple columns. Features with keys that are
    not of interest are removed, just like those that have unsure or joined locations.

    Args:
        spark: The SparkSession to use.
        file: The file to read the features from.

    Returns:
        A filtered DataFrame with the features from the file each as their own row.
    """
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
    """Removes gene features that have a CDS feature within or spanning their location.

    It removes gene features when a CDS feature matching these conditions is present:
        - they're both from the same GBFF record,
        - they have the same location or the CDS is located within that of the gene,
        - if they are both complement or not.

    Args:
        features_df: The DataFrame with the features to filter.

    Returns:
        A DataFrame without all the features that have a CDS feature within or spanning
        their location, meaning all remaining gene features are non-coding.
    """
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


def question1(features_df: DataFrame) -> float:
    """How many features does an Archaea genome have on average?

    To answer this question it calculates the average amount of features an Archaea
    genome has by grouping the features by the genome/record identifier and counting
    them. The mean of all these counts is then calculated and returned.

    Args:
        features_df: The DataFrame with the features to calculate the average from.

    Returns:
        The average amount of features an Archaea genome (record) has.
    """
    return (
        features_df
        .groupBy("identifier")
        .count()
        .agg({"count": "mean"})
        .first()[0]
    )


def question2(features_df: DataFrame) -> float:
    """What is the proportion between coding and non-coding features?

    To answer this question it first gets two new dataframes, one with only the coding
    features and one with only the non-coding features. Of these two the amount of
    features is counted and the proportion between the two is calculated and returned.

    Args:
        features_df: The DataFrame with the features to calculate the average from.

    Returns:
        The proportion of coding to non-coding features.
    """
    coding_features = features_df.filter(features_df.key.isin(CODING_KEYS))
    non_coding_features = features_df.filter(features_df.key.isin(NON_CODING_KEYS))
    return coding_features.count() / non_coding_features.count()


def question3(features_df: DataFrame) -> tuple[int, int]:
    """What are the min and max amount of proteins of all organisms in the file?

    To answer this question it first filters to only have coding features and then
    groups by the organism they belong to. The amount of features for each organism is
    counted and the minimum and maximum amount of proteins are extracted and returned.

    Args:
        features_df: The DataFrame with the features to calculate the average from.

    Returns:
        A tuple with the minimum and maximum amount of proteins of all organisms.
    """
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
    """What is the average length of a feature?

    To answer this question it calculates the length of each feature by subtracting
    their start position from their stop position and then the mean of all the lengths
    is calculated and returned.

    Args:
        features_df: The DataFrame with the features to calculate the average from.

    Returns:
        The average length of a feature.
    """
    return (
        features_df
        .withColumn("length", col("stop") - col("start"))
        .agg({"length": "mean"})
        .first()[0]
    )


def answer_questions(features_df: DataFrame) -> None:
    """Calls the question functions to get the answers and prints them to the console.

    Args:
        features_df: The DataFrame with the features to calculate the answers from.
    """
    # Q1: How many features does an Archaea genome have on average?
    answer1 = question1(features_df)
    print(f"Answer 1: An Archaea genome has {answer1:.2f} features on average.")

    # Q2: What is the proportion between coding and non-coding features?
    answer2 = question2(features_df)
    print(
        f"Answer 2: "
        f"The proportion between coding and non-coding features is {answer2:.2f} to 1."
    )

    # Q3: What are the min and max amount of proteins of all organisms in the file?
    answer3_min, answer3_max = question3(features_df)
    print(
        f"Answer 3: "
        f"The minimum amount of proteins in any organism is {answer3_min} and "
        f"the maximum is {answer3_max}."
    )

    # Q4: What is the average length of a feature?
    answer4 = question4(features_df)
    print(f"Answer 4: The average length of a feature is {answer4:.2f}.")


# MAIN
def main():
    """Main function of the script."""
    # Create SparkSession
    spark: SparkSession = (
        SparkSession.builder
        .master("local[16]")
        .config("spark.ui.enabled", False)
        .config("spark.executor.memory", "64g")
        .config("spark.driver.memory", "64g")
        # .config("spark.log.level", "ERROR")
        .appName("bdc_a5")
        .getOrCreate()
    )

    # Specify the directory to the data and which file to use
    data_dir = Path("/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release")
    file = data_dir / "archaea" / "archaea.1.genomic.gbff"
    # file = data_dir / "archaea" / "archaea.2.genomic.gbff"
    # file = data_dir / "archaea" / "archaea.3.genomic.gbff"
    print("\nPerforming analysis and answering questions for the following file:")
    print(f"\t{file}\n")

    # Create a DataFrame with a filtered subset of features from the .gbff file
    features_df: DataFrame = create_features_dataframe(spark, str(file))
    features_df = remove_coding_gene_features(features_df)

    # Answer the questions about the features
    answer_questions(features_df)

    # Save the DataFrame in Spark format with only coding features
    output_file = file.stem + "_coding_features.parquet"
    features_df.filter(col("key").isin(CODING_KEYS)).write.parquet(output_file)
    print("\nSaved the DataFrame with only the coding features to:")
    print(f"\t{output_file}\n")


if __name__ == "__main__":
    main()
