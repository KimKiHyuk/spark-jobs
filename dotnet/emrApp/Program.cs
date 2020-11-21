using System;

namespace emrApp
{
    class Program
    {
        static void Main(string[] args)
        {
            SparkSession spark = SparkSession
                .Builder()
                .AppName("word_count_sample")
                .GetOrCreate();

        }
    }
}
