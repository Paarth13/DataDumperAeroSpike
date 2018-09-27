using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Aerospike;
using Aerospike.Client;
using System.IO;
using System.Data;
using CsvHelper;

namespace DataDumpDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            var client = new AerospikeClient("18.235.70.103",3000);
            string nameSpace = "AirEngine";
            string setName = "Paarth";

            string filepath = @"C:\Users\pvashisht\Desktop\2018-08-charlottesville-twitter-trolls\data\tweets4.csv";
            StreamReader sr = new StreamReader(filepath);
            //Csv reader reads the stream
            CsvReader csvread = new CsvReader(sr);

            //csvread will fetch all record in one go to the IEnumerable object record
            IEnumerable<Trolls> record = csvread.GetRecords<Trolls>();
            int count = 0;
            foreach (var rec in record) // Each record will be fetched and printed on the screen
            {
                var key = new Key(nameSpace, setName, rec.tweet_id);
                count++;

                client.Put(new WritePolicy(), key, new Bin[] { new Bin("author", rec.author), new Bin("Content", rec.content), new Bin("Region", rec.region), new Bin("Language", rec.language), new Bin("tweetDate", rec.tweet_date), new Bin("Tweet_time", rec.tweet_time), new Bin("Year", rec.year), new Bin("Month", rec.month), new Bin("Hour", rec.hour), new Bin("Minute", rec.minute), new Bin("following", rec.following), new Bin("Follower", rec.followers), new Bin("Post_url", rec.post_url), new Bin("Post_Type", rec.post_type), new Bin("Retweet", rec.retweet), new Bin("tweet_Id", rec.tweet_id), new Bin("Author_id", rec.author_id), new Bin("acnt_catgry",rec.account_category ), new Bin("new_june_2018",rec.new_june_2018 ) });
                if (count == 2000)
                    break;
            }
            sr.Close();

        }
    }
}
