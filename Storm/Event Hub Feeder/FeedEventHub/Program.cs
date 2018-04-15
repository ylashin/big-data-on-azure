using Microsoft.Azure.EventHubs;
using System;
using System.Configuration;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace FeedEventHub
{
    class Program
    {
        static void Main(string[] args)
        {
            var client = EventHubClient.CreateFromConnectionString(ConfigurationManager.AppSettings["EvenHubConnectionString"]);
            var sampleFile = ConfigurationManager.AppSettings["SampleFile"];
            Console.WriteLine($"Downloading data for {sampleFile}");
            WebClient wc = new WebClient();
            var fileName = Path.GetFileName(sampleFile);
            wc.DownloadFile(sampleFile, fileName);

            var lines = File.ReadAllLines(fileName);

            Console.WriteLine("Sending data...");
            Console.WriteLine(DateTime.Now);
            var index = 0;
            var sb = new StringBuilder();
            var maxBatchSize = 240 * 1024; // HACK : it's actually 256KB but keep some buffer for overhead and so on
            do
            {
                if (sb.Length + lines[index].Length < maxBatchSize)
                {
                    sb.AppendLine(lines[index]);
                }
                else
                {
                    HandleBatch(client, sb).Wait();
                    double percentComplete = ( 1.0 * index / lines.Length) * 100.0;
                    Console.WriteLine($"Processed {percentComplete:0.0}%");

                    sb.AppendLine(lines[index]);
                }
                index++;
            }
            while (index < lines.Length);
            
            if(sb.Length>0)
            {
                HandleBatch(client, sb).Wait();
            }

            Console.WriteLine(DateTime.Now);

        }

        private static async Task HandleBatch(EventHubClient _Client, StringBuilder sb)
        {
            var payload = Encoding.UTF8.GetBytes(sb.ToString());
            var batch = new EventData(payload);
            await _Client.SendAsync(batch);
            sb.Clear();
        }        
    }
}
