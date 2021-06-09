using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;
using System.Net;
using System.IO;

namespace RunTimeTriggerExchangeRatesAPI
{
    public static class ExchangeRatesAPIFunc
    {
        [FunctionName("ExchangeRatesAPIFunc")]
        public static void Run([TimerTrigger("0 0 7 * * *", RunOnStartup = true)] TimerInfo myTimer, ILogger log)
        {
            string exchangeRatesDate = DateTime.Now.ToString("yyyy-MM-dd");
            string accessKey = Environment.GetEnvironmentVariable("API_ACCESS_KEY");
            string exchangeRatesAPIURL = $"http://api.exchangeratesapi.io/v1/{exchangeRatesDate}?access_key={accessKey}&format=1";

            //The storage connection string is stored in an local.settings.json
            string targetStorageConnectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING");
            string targetContainerName = Environment.GetEnvironmentVariable("TARGET_CONTAINER_NAME");
            string targetBlobName = $"{DateTime.Now:yyyyMMddHHmmssfff}.json";

            log.LogInformation($"Retrieve exchange rates as of '{exchangeRatesDate}' to '{targetBlobName}' blob in '{targetContainerName}' container");

            using (WebClient webClient = new WebClient())
            using (Stream httpStream = webClient.OpenRead(exchangeRatesAPIURL))
            {
                BlobServiceClient blobServiceClient = new BlobServiceClient(targetStorageConnectionString);

                // Get a container client object
                BlobContainerClient targetContainerClient = blobServiceClient.GetBlobContainerClient(targetContainerName);

                // Get a reference to the blob
                BlobClient blobClient = targetContainerClient.GetBlobClient(targetBlobName);
                blobClient.Upload(httpStream, true);
            }
        }
    }
}

