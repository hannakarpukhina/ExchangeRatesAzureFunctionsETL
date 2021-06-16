using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Collections.Generic;

namespace LoadExchangeRate
{
    public static class ExchangeRatesAPILoad
    {
        static readonly string SourceContainerName = Environment.GetEnvironmentVariable("TARGET_CONTAINER_NAME");

        [FunctionName("ExchangeRatesAPILoad")]
        public static void Run([BlobTrigger("%TARGET_CONTAINER_NAME%/{name}", Connection = "AzureWebJobsStorage")]Stream myBlob, string name, ILogger log)
        {
            try
            {
                log.LogInformation($"C# Blob trigger function Processed blob\n Name: {name}");
                var dbClient = new MongoClient(Environment.GetEnvironmentVariable("CONNECTION_STRING_MONGODB"));
                var database = dbClient.GetDatabase(Environment.GetEnvironmentVariable("DATABASE"));
                var collection = database.GetCollection<ExchangeRatesAPIItem>(Environment.GetEnvironmentVariable("COLLECTION"));

                var jsonFileString = "";
                using (var streamReader = new StreamReader(myBlob))
                {
                    jsonFileString = streamReader.ReadToEnd();
                }

                var exchangeRatesAPIItem = JsonSerializer.Deserialize<ExchangeRatesAPIItem>(jsonFileString);
                exchangeRatesAPIItem._id = string.Concat(exchangeRatesAPIItem.@base, "-", exchangeRatesAPIItem.date);

                var filter = Builders<ExchangeRatesAPIItem>.Filter.Eq("_id", exchangeRatesAPIItem._id);
                var result = collection.Find(filter);
                if (!result.Any())
                {
                    collection.InsertOne(exchangeRatesAPIItem);
                }
            }
            catch (Exception e)
            {
                log.LogError($"Error: failed to process blob: {SourceContainerName}\\{name}\n Error message: {e.Message}\n StackTrace: {e.StackTrace}");
                throw;
            }
        }
    }
    public class ExchangeRatesAPIItem
    {
        public string _id { get; set; }
        public bool success { get; set; }
        public int timestamp { get; set; }
        public string @base { get; set; }
        public string date { get; set; }
        public Dictionary<string, double> rates { get; set; }
    }
}
