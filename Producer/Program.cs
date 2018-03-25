using RabbitMQFactory;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Xml;
using System.Xml.Linq;


namespace Producer
{
    class Program
    {
        static volatile bool _cancelling;
      

        public static RabbitMQConnectionFactory Instance
        {
            get
            {
                RabbitMQConnectionFactory Instance = RabbitMQConnectionFactory.RabbitMQConnectionFactoryInstance;
                return Instance;
            }
        }
        static void Main(string[] args)
        {
          
            Instance.CreateFanoutExchange("SendMessages", false, true);
            var thread = new Thread(() => PublishQuotes());
            thread.Start();

            Console.WriteLine("Press 'x' to exit");
            var input = (char)Console.Read();
            _cancelling = true;

       
        }

    

        static void PublishQuotes()
        {
            while (true)
            {
                if (_cancelling) return;
                System.Collections.IEnumerable quotes = FetchStockQuotes(new[] { "GOOG", "HD", "MCD" });
                foreach (string quote in quotes)
                {
                    byte[] message = Encoding.UTF8.GetBytes(quote);
                    bool isPublished=Instance.PublishMessage("SendMessages", "", false, null, message,0);
                }
                Thread.Sleep(5000);
            }
        }


        static IEnumerable<string> FetchStockQuotes(string[] symbols)
        {
            var quotes = new List<string>();

            quotes.Add("wahid");
            quotes.Add("abdul");

            return quotes;
        }
    }

   
    
}
