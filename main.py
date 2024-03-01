from pyspark.sql import SparkSession

from src.utils import read_yaml
from src.us_vehicle_accident_analysis import USVehicleAccidentAnalysis

if __name__ == "__main__":
    # Initialize spark session
    spark = SparkSession.builder.appName("USVehicleAccidentAnalysis").getOrCreate()

    config_file_name = "config.yaml"
    spark.sparkContext.setLogLevel("ERROR")

    config = read_yaml(config_file_name)
    output_file_paths = config.get("OUTPUT_PATH")
    file_format = config.get("FILE_FORMAT")

    usvaa = USVehicleAccidentAnalysis(spark, config)

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
    # offenses (to be deduced from the data)
    print(
        "8. Result:",
        usvaa.get_top_5_vehicle_brand(
            output_file_paths.get(8), file_format.get("Output")
        ),
    )

    spark.stop()
