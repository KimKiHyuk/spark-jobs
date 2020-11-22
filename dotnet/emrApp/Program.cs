using System;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using System.Collections.Generic;

namespace emrApp
{
    class Program
    {
        private class Address 
        {
            public Address(string[] raw) 
            {
                city = raw[0];
                state = raw[1];
                postal = raw[2];
            }
            string city;
            string state;
            string postal;

            public object[] toArray()
            {
                return new object[] {city, state, postal};
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

            var rows = dataFrame.Select("address").Collect();

            var seq = new List<GenericRow>();

            foreach (var row in rows) {
                var str = row.ToString();
                var address = new Address(str.Substring(1, str.Length - 2).Split(','));
                seq.Add(new GenericRow(address.toArray()));
            }

            var schema = new StructType(
                new List<StructField>() {
                    new StructField("city", new StringType()),
                    new StructField("state", new StringType()),
                    new StructField("postal", new StringType())
                }
            );

            var addressDataFrame = spark.CreateDataFrame(seq, schema);
            addressDataFrame.Show();

            addressDataFrame.GroupBy("city").Count();
            addressDataFrame.Show();
            addressDataFrame.Write().Format("json").Save($"{args[1]}/{DateTime.UtcNow.ToString().Replace(" ", "-")}");
        }
    }
}
