from pyspark.sql import SparkSession
import argparse


def main(date):
    # Creating the Spark session
    spark = SparkSession.builder.appName("my-2nd-project-parameter-backfilling").getOrCreate()

    # Defining the path where the files are located
    input_path = f"gs://airflow-project-exciess/Project-2/input/data/orders_{date}.csv"

    # Reading the CSV file
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Filtering the DataFrame
    df_filtered = df.filter(df.orders_status == 'Completed')

    # Writing the data back to GCS with appropriate naming
    output_path = f"gs://airflow-project-exciess/Project-2/output/processed_orders_{date}"
    df_filtered.write.csv(output_path, mode="overwrite", header=True)

    # Stopping the Spark session
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process date argument')
    parser.add_argument('--date', type=str, required=True, help='Date in yyyymmdd format')
    args = parser.parse_args()
    main(args.date)
