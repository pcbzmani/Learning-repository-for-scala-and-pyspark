from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class LatLongCalc():
    def cal_lat_log_dist(self, df, lat1, long1, lat2, long2):
        # Ref - https://en.wikipedia.org/wiki/Great-circle_distance#Formulae
        # We are using haversine formaula to derive this Distance between two Co-ordinates
        # Parameters:
        # Base DF with Four columns where it has LAT and LONG
        # Corresponding column name in Dataframe -> Cororidnate1 LAT1 LONG1
        # Corresponding column name in Dataframe -> Cororidnate2 LAT2 LONG2

        df = df.withColumn('distance_in_kms' , \
            F.round((F.acos((F.sin(F.radians(F.col(lat1))) * F.sin(F.radians(F.col(lat2)))) + \
                   ((F.cos(F.radians(F.col(lat1))) * F.cos(F.radians(F.col(lat2)))) * \
                    (F.cos(F.radians(long1) - F.radians(long2))))
                       ) * F.lit(6371.0)), 4))
        return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Distance Calculation").getOrCreate()

    # Create a DataFrame with the coordinates
    data = [("A", 52.370216, 4.895168, "B", 51.507351, -0.127758),
            ("C", 48.856614, 2.352222, "D", 40.712776, -74.005974),
            ("E", 37.774929, -122.419416, "F", 33.868820, 151.209296)]

    df = spark.createDataFrame(data, ["coord1", "lat1", "long1", "coord2", "lat2", "long2"])

    # Calculate the distance between the coordinates
    calc = LatLongCalc()
    df = calc.cal_lat_log_dist(df, "lat1", "long1", "lat2", "long2")

    # Show the results
    df.show()
