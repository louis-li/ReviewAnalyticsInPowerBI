using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.IO;
using System.Data.SqlClient;
using System.Threading;
using Microsoft.Azure.CognitiveServices.Language.TextAnalytics;
using Microsoft.Azure.CognitiveServices.Language.TextAnalytics.Models;
using Microsoft.Azure.EventHubs;
using System.Net;


namespace LoadReviewToSQL
{

    //create table Review
    //(
    //reviewID int identity(1,1) primary key,
    //reviewerID varchar(200) not null ,
    //asin varchar(100) not null,
    //reviewerName varchar(500),
    //helpful int,
    //not_helpful int,
    //reviewText varchar(max),
    //overall decimal,
    //summary varchar(max),
    //unixReviewTime bigint,
    //reviewTime datetime2,
    //KeyPhrases varchar(max)
    //)

    public class ReviewObject
    {
        [JsonProperty("reviewerID")]
        public string reviewerID { get; set; }
        [JsonProperty("asin")]
        public string asin { get; set; }
        [JsonProperty("reviewerName")]
        public string reviewerName { get; set; }
        [JsonProperty("helpful")]
        public IList<int> helpful { get; set; }
        [JsonProperty("reviewText")]
        public string reviewText { get; set; }
        [JsonProperty("overall")]
        public decimal overall { get; set; }
        [JsonProperty("summary")]
        public string summary { get; set; }
        [JsonProperty("unixReviewTime")]
        public int unixReviewTime { get; set; }
        [JsonProperty("reviewTime")]
        public DateTime reviewTime { get; set; }
        public string KeyPhrases { get; set; }

        public ReviewObject()
        {
            reviewerName = string.Empty;
            reviewText = string.Empty;
            reviewerName = string.Empty;
            overall = 0;
            summary = string.Empty;
            KeyPhrases = string.Empty;


        }
        public string ToJson()
        {
            return JsonConvert.SerializeObject(this);
        }

    }
    class Program
    {
        private static EventHubClient eventHubClient;

        //Need to enter key here from Azure Event Hub
        private const string EhConnectionString = "Endpoint=sb://mytesteventhubs.servicebus.windows.net/;SharedAccessKeyName=policy_myfirsteventhub;SharedAccessKey=<Enter key here>;EntityPath=myfirsteventhub";
        private const string EhEntityPath = "myfirsteventhub";

        static void Main(string[] args)
        {
            //init database configuration
            string cmdString = "INSERT INTO Review (reviewerID, asin, reviewerName,helpful, not_helpful, reviewText,overall,summary,unixReviewTime,reviewTime,KeyPhrases) " +
                "VALUES (@reviewerID, @asin, @reviewerName, @helpful, @not_helpful, @reviewText,@overall,@summary,@unixReviewTime,@reviewTime,@KeyPhrases)";
            string connection = "Server=pbi-customerreivew.database.windows.net;Database=customerreview;User Id=louisli;Password = longP@$$w0rd1; ";

            //init Cognitive API
            // Create a client.
            ITextAnalyticsAPI client = new TextAnalyticsAPI();
            client.AzureRegion = AzureRegions.Westcentralus;
            client.SubscriptionKey = "<Get Key from Azure Text Analytics API and paste it here>";

            //Init Event Hub
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EhConnectionString)
            {
                EntityPath = EhEntityPath
            };

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());


            //Start loading reviews
            using (SqlConnection conn = new SqlConnection(connection))
            {
                conn.Open();
                foreach (string line in File.ReadLines(@"D:\Training\reviews_Musical_Instruments_5.json\Musical_Instruments_5_reduced.json"))
                {
                    //Read one record from JSON file
                    ReviewObject review = JsonConvert.DeserializeObject<ReviewObject>(line);
                    // Get key words
                    KeyPhraseBatchResult result2 = client.KeyPhrases(
                        new MultiLanguageBatchInput(
                            new List<MultiLanguageInput>()
                            {
                                new MultiLanguageInput("en", "1", review.reviewText),
                                new MultiLanguageInput("en", "2", review.summary)
                            }));

                    foreach (var document in result2.Documents)
                    {
                        foreach (string keyphrase in document.KeyPhrases)
                        {
                            review.KeyPhrases += keyphrase +",";
                        }
                    }
                    review.KeyPhrases = review.KeyPhrases.Substring(0, review.KeyPhrases.Length - 1);

                    //Load into database
                    using (SqlCommand comm = new SqlCommand())
                    {
                        comm.Connection = conn;
                        comm.CommandText = cmdString;
                        comm.Parameters.AddWithValue("@reviewerID", review.reviewerID);
                        comm.Parameters.AddWithValue("@asin", review.asin);
                        comm.Parameters.AddWithValue("@reviewerName", review.reviewerName);
                        comm.Parameters.AddWithValue("@helpful", review.helpful[0]);
                        comm.Parameters.AddWithValue("@not_helpful", review.helpful[1]);
                        comm.Parameters.AddWithValue("@reviewText", review.reviewText);
                        comm.Parameters.AddWithValue("@overall", review.overall);
                        comm.Parameters.AddWithValue("@summary", review.summary);
                        comm.Parameters.AddWithValue("@unixReviewTime", review.unixReviewTime);
                        comm.Parameters.AddWithValue("@reviewTime", review.reviewTime);
                        comm.Parameters.AddWithValue("@KeyPhrases", review.KeyPhrases);

                        try
                        {

                            comm.ExecuteNonQuery();
                        }
                        catch(SqlException e)
                        {
                            // do something with the exception
                            // don't hide it
                        }
                    }


                    //Send review to Event Hub
                    EventData msg = new EventData(Encoding.UTF8.GetBytes(review.ToJson()));
                    eventHubClient.SendAsync(msg);
                    Thread.Sleep(3000);

                }
            }


        }
    }
}
