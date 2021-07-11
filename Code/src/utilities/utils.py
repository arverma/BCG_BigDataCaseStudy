import yaml


def load_csv_data_to_df(spark, file_path):
    """
    Read CSV data
    :param spark: spark instance
    :param file_path: path to the csv file
    :return: dataframe
    """
    return spark.read.option("inferSchema", "true").csv(file_path, header=True)


def read_yaml(file_path):
    """
    Read Config file in YAML format
    :param file_path: file path to config.yaml
    :return: dictionary with config details
    """
    with open(file_path, "r") as f:
        return yaml.safe_load(f)


def write_output(df, file_path, write_format):
    """
    Write data frame to csv
    :param write_format: Write file format
    :param df: dataframe
    :param file_path: output file path
    :return: None
    """
    # df = df.coalesce(1)
    df.write.format(write_format).mode('overwrite').option("header", "true").save(file_path)
