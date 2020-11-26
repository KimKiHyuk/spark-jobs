using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace emrApp
{
    interface IDataFrame {
        StructType GetSchema();
    }

    class Program
    {
        private class Region: IDataFrame
        {
            
            int regionCode;
            
            public StructType GetSchema()
            {
                return new StructType(
                    new List<StructField>() {
                        new StructField("city", new StringType()),
                        new StructField("count", new IntegerType()),
                    }
                );
            }
        }
        static void Main(string[] args)
        {
            SparkSession spark = SparkSession
                .Builder()
                .AppName("emrApp")
                .GetOrCreate();


            DataFrame dataFrame = spark
                .Read()
                .Format("avro")
                .Load(args[0]);

            var s = dataFrame["itemid"];
        
            Func<Column, Column> udf = Udf<string, string[]>(
                str => str.Split('_')
            );  // city_23 --> 23 --> 2

            dataFrame = dataFrame.WithColumn("Region", udf(dataFrame["itemid"]));
            dataFrame.Show();

            //spark.Stop();
        }
    }
}
