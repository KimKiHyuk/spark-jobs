using System;
using Microsoft.Spark.Sql;

namespace emrApp
{
    class Program
    {
        static void Main(string[] args)
        {
            SparkSession spark = SparkSession
                .Builder()
                .AppName("emrApp")
                .GetOrCreate();


            DataFrame dataFrame = spark
                .Read()
                .Format("avro")
                .Load("s3a://spark-data-vjal1251/topics/orders/partition=0");
            
            var rows = dataFrame.Select("address").Collect();

            foreach (var row in rows) {
                Console.WriteLine(row);
            }
        }
    }
}
