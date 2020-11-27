using System;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using emrApp.Model;

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
                .Load(args[0]);

            RegionModel regionModel = new RegionModel();
            
            Func<Column, Column> udfConvertRegion = Udf<string, string>(
                city => {                    
                    var regionCode = city.Split('_')[1].Substring(0, 1);
                    var convertedRegion = String.Empty; 
                    regionModel.ConversionTable.TryGetValue(regionCode, out convertedRegion);
                    return convertedRegion;
                } // city_23 --> 23 --> 2 --> {2 : Brisbane} --> ** Brisbane **
            );

            dataFrame = dataFrame
                .WithColumn("Region", udfConvertRegion(dataFrame["address.city"]))
                .Drop("orderunits", "address");
            
            dataFrame
                .Coalesce(1)
                .Write()
                .Format("csv")
                .Save($"{args[1]}/{DateTime.UtcNow.ToString("yyyy/MM/dd/hh-mm-ss")}");  
        }
    }
}
