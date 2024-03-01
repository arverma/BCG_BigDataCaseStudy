from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number

from src.utils import load_csv_data_to_df, read_yaml, write_output


class USVehicleAccidentAnalysis:
    def __init__(self, path_to_config_file):
        input_file_paths = read_yaml(path_to_config_file).get("INPUT_FILENAME")
        self.df_charges = load_csv_data_to_df(spark, input_file_paths.get("Charges"))
        self.df_damages = load_csv_data_to_df(spark, input_file_paths.get("Damages"))
        self.df_endorse = load_csv_data_to_df(spark, input_file_paths.get("Endorse"))
        self.df_primary_person = load_csv_data_to_df(
            spark, input_file_paths.get("Primary_Person")
        )
        self.df_units = load_csv_data_to_df(spark, input_file_paths.get("Units"))
        self.df_restrict = load_csv_data_to_df(spark, input_file_paths.get("Restrict"))

    def count_male_accidents(self, output_path, output_format):
        """
        Finds the crashes (accidents) in which number of persons killed are male
        :param output_path: output file path
        :param output_format: Write file format
        :return: dataframe count
        """
        df = self.df_primary_person.filter(
            self.df_primary_person.PRSN_GNDR_ID == "MALE"
        )
        write_output(df, output_path, output_format)
        return df.count()

    def count_2_wheeler_accidents(self, output_path, output_format):
        """
        Finds the crashes where the vehicle type was 2 wheeler.
        :param output_format: Write file format
        :param output_path: output file path
        :return: dataframe count
        """
        df = self.df_units.filter(col("VEH_BODY_STYL_ID").contains("MOTORCYCLE"))
        write_output(df, output_path, output_format)

        return df.count()

    def get_state_with_highest_female_accident(self, output_path, output_format):
        """
        Finds state name with the highest female accidents
        :param output_format: Write file format
        :param output_path: output file path
        :return: state name with the highest female accidents
        """
        df = (
            self.df_primary_person.filter(
                self.df_primary_person.PRSN_GNDR_ID == "FEMALE"
            )
            .groupby("DRVR_LIC_STATE_ID")
            .count()
            .orderBy(col("count").desc())
        )
        write_output(df, output_path, output_format)

        return df.first().DRVR_LIC_STATE_ID

    def get_top_vehicle_contributing_to_injuries(self, output_path, output_format):
        """
        Finds Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        :param output_format: Write file format
        :param output_path: output file path
        :return: Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        """
        df = (
            self.df_units.filter(self.df_units.VEH_MAKE_ID != "NA")
            .withColumn("TOT_CASUALTIES_CNT", self.df_units[35] + self.df_units[36])
            .groupby("VEH_MAKE_ID")
            .sum("TOT_CASUALTIES_CNT")
            .withColumnRenamed("sum(TOT_CASUALTIES_CNT)", "TOT_CASUALTIES_CNT_AGG")
            .orderBy(col("TOT_CASUALTIES_CNT_AGG").desc())
        )

        df_top_5_to_15 = df.limit(15).subtract(df.limit(5))
        write_output(df_top_5_to_15, output_path, output_format)

        return [veh[0] for veh in df_top_5_to_15.select("VEH_MAKE_ID").collect()]

    def get_top_ethnic_ug_crash_for_each_body_style(self, output_path, output_format):
        """
        Finds and show top ethnic user group of each unique body style that was involved in crashes
        :param output_format: Write file format
        :param output_path: output file path
        :return: None
        """
        w = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
        df = (
            self.df_units.join(self.df_primary_person, on=["CRASH_ID"], how="inner")
            .filter(
                ~self.df_units.VEH_BODY_STYL_ID.isin(
                    ["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"]
                )
            )
            .filter(~self.df_primary_person.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"]))
            .groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
            .count()
            .withColumn("row", row_number().over(w))
            .filter(col("row") == 1)
            .drop("row", "count")
        )

        write_output(df, output_path, output_format)

        df.show(truncate=False)

    def get_top_5_zip_codes_with_alcohols_as_cf_for_crash(
        self, output_path, output_format
    ):
        """
        Finds top 5 Zip Codes with the highest number crashes with alcohols as the contributing factor to a crash
        :param output_format: Write file format
        :param output_path: output file path
        :return: List of Zip Codes
        """
        df = (
            self.df_units.join(self.df_primary_person, on=["CRASH_ID"], how="inner")
            .dropna(subset=["DRVR_ZIP"])
            .filter(
                col("CONTRIB_FACTR_1_ID").contains("ALCOHOL")
                | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")
            )
            .groupby("DRVR_ZIP")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )
        write_output(df, output_path, output_format)

        return [row[0] for row in df.collect()]

    def get_crash_ids_with_no_damage(self, output_path, output_format):
        """
        Counts Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4
        and car avails Insurance.
        :param output_format: Write file format
        :param output_path: output file path
        :return: List of crash ids
        """
        df = (
            self.df_damages.join(self.df_units, on=["CRASH_ID"], how="inner")
            .filter(
                (
                    (self.df_units.VEH_DMAG_SCL_1_ID > "DAMAGED 4")
                    & (
                        ~self.df_units.VEH_DMAG_SCL_1_ID.isin(
                            ["NA", "NO DAMAGE", "INVALID VALUE"]
                        )
                    )
                )
                | (
                    (self.df_units.VEH_DMAG_SCL_2_ID > "DAMAGED 4")
                    & (
                        ~self.df_units.VEH_DMAG_SCL_2_ID.isin(
                            ["NA", "NO DAMAGE", "INVALID VALUE"]
                        )
                    )
                )
            )
            .filter(self.df_damages.DAMAGED_PROPERTY == "NONE")
            .filter(self.df_units.FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE")
        )
        write_output(df, output_path, output_format)

        return [row[0] for row in df.collect()]

    def get_top_5_vehicle_brand(self, output_path, output_format):
        """
        Determines the Top 5 Vehicle Makes/Brands where drivers are charged with speeding related offences, has licensed
        Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
        offences
        :param output_format: Write file format
        :param output_path: output file path
        :return List of Vehicle brands
        """
        top_25_state_list = [
            row[0]
            for row in self.df_units.filter(
                col("VEH_LIC_STATE_ID").cast("int").isNull()
            )
            .groupby("VEH_LIC_STATE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(25)
            .collect()
        ]
        top_10_used_vehicle_colors = [
            row[0]
            for row in self.df_units.filter(self.df_units.VEH_COLOR_ID != "NA")
            .groupby("VEH_COLOR_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(10)
            .collect()
        ]

        df = (
            self.df_charges.join(self.df_primary_person, on=["CRASH_ID"], how="inner")
            .join(self.df_units, on=["CRASH_ID"], how="inner")
            .filter(self.df_charges.CHARGE.contains("SPEED"))
            .filter(
                self.df_primary_person.DRVR_LIC_TYPE_ID.isin(
                    ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
                )
            )
            .filter(self.df_units.VEH_COLOR_ID.isin(top_10_used_vehicle_colors))
            .filter(self.df_units.VEH_LIC_STATE_ID.isin(top_25_state_list))
            .groupby("VEH_MAKE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )

        write_output(df, output_path, output_format)

        return [row[0] for row in df.collect()]


if __name__ == "__main__":
    # Initialize sparks session
    spark = SparkSession.builder.appName("USVehicleAccidentAnalysis").getOrCreate()

    config_file_name = "config.yaml"
    spark.sparkContext.setLogLevel("ERROR")

    usvaa = USVehicleAccidentAnalysis(config_file_name)
    output_file_paths = read_yaml(config_file_name).get("OUTPUT_PATH")
    file_format = read_yaml(config_file_name).get("FILE_FORMAT")

    # 1. Find the number of crashes (accidents) in which number of persons killed are male?
    print(
        "1. Result:",
        usvaa.count_male_accidents(output_file_paths.get(1), file_format.get("Output")),
    )

    # 2. How many two-wheelers are booked for crashes?
    print(
        "2. Result:",
        usvaa.count_2_wheeler_accidents(
            output_file_paths.get(2), file_format.get("Output")
        ),
    )

    # 3. Which state has the highest number of accidents in which females are involved?
    print(
        "3. Result:",
        usvaa.get_state_with_highest_female_accident(
            output_file_paths.get(3), file_format.get("Output")
        ),
    )

    # 4. Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    print(
        "4. Result:",
        usvaa.get_top_vehicle_contributing_to_injuries(
            output_file_paths.get(4), file_format.get("Output")
        ),
    )

    # 5. For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    print("5. Result:")
    usvaa.get_top_ethnic_ug_crash_for_each_body_style(
        output_file_paths.get(5), file_format.get("Output")
    )

    # 6. Among the crashed cars, what are the Top 5 Zip Codes with the highest number of crashes with alcohol as the
    # contributing factor to a crash (Use Driver Zip Code)
    print(
        "6. Result:",
        usvaa.get_top_5_zip_codes_with_alcohols_as_cf_for_crash(
            output_file_paths.get(6), file_format.get("Output")
        ),
    )

    # 7. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4
    # and car avails Insurance
    print(
        "7. Result:",
        usvaa.get_crash_ids_with_no_damage(
            output_file_paths.get(7), file_format.get("Output")
        ),
    )

    # 8. Determine the Top 5 Vehicle Makes/Brands where drivers are charged with speeding-related offenses, has licensed
    # Drivers, uses top 10 used vehicle colors and has a car licensed in the Top 25 states with the highest number of
    # offences (to be deduced from the data)
    print(
        "8. Result:",
        usvaa.get_top_5_vehicle_brand(
            output_file_paths.get(8), file_format.get("Output")
        ),
    )

    spark.stop()
