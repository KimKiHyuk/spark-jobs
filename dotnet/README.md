## .NET for Apache spark

```
dotnet build
spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.6 --class org.apache.spark.deploy.dotnet.DotnetRunner --master local bin/Debug/netcoreapp3.1/microsoft-spark-2-4_2.11-1.0.0.jar dotnet bin/Debug/netcoreapp3.1/emrApp.dll s3a://spark-data-vjal1251/topics/orders/partition=0 s3a://spark-data-vjal1251/result
```