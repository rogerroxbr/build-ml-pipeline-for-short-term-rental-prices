#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning,
exporting the result to a new artifact
"""
import os
import argparse
import logging

import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def basic_clean(args):
    """
    Function to download data from W&B, apply basic data cleaning,
    and logging
    argument:
        args : command line argument to specify artifact information and
            basic cleaning configuration
    return:
        None
    """

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    logger.info("Download input artifact")
    artifact = run.use_artifact(args.input_artifact)
    artifact_path = artifact.file()

    logger.info("Loading artifact to dataframe")
    # read artifact
    df = pd.read_csv(artifact_path)

    logger.info("Cleaning the data")
    df["last_review"] = pd.to_datetime(df["last_review"])
    idx = df["price"].between(args.min_price, args.max_price)
    df = df[idx].copy()

    idx = df["longitude"].between(-74.25, -73.50) & df["latitude"].between(
        40.5, 41.2
    )
    df = df[idx].copy()

    filename = args.output_artifact
    df.to_csv(filename, index=False)

    logger.info("Creating artifact")
    artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )

    artifact.add_file(filename)

    logger.info("Logging artifact")
    run.log_artifact(artifact)

    os.remove(filename)
    logger.info("Cleaned data artifact logged to W&B")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help="Name of input artifact",
        required=True,
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Name of output artifact",
        required=True,
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="Type of output artifact",
        required=True,
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="Description of the output artifact",
        required=True,
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="Minimum number for a price",
        required=True,
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="Maximum number for a price",
        required=True,
    )

    args = parser.parse_args()

    basic_clean(args)
