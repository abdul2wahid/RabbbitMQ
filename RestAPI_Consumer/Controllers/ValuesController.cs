using RabbitMQFactory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Web.Http;

namespace RestAPI_Consumer.Controllers
{
  
    public class ValuesController : ApiController
    {
        // GET api/values
        public void Get()
        {



            RabbitMQConnectionFactory Instance = RabbitMQConnectionFactory.RabbitMQConnectionFactoryInstance;

            Instance.CreateFanoutExchange("SendMessages", true, false);

            System.Collections.IEnumerable quotes = FetchStockQuotes(new[] { "GOOG", "HD", "MCD" });
            foreach (string quote in quotes)
            {
                byte[] message = Encoding.UTF8.GetBytes(quote);
                bool isPublished = Instance.PublishMessage("SendMessages", "", false, null, message,0);
            }
        }

         IEnumerable<string> FetchStockQuotes(string[] symbols)
        {
            var quotes = new List<string>();

            quotes.Add("wahid");
            quotes.Add("abdul");

            return quotes;
        }
        // GET api/values/5
        public string Get(int id)
        {
            return "value";
        }

        // POST api/values
        public void Post([FromBody]string value)
        {
        }

        // PUT api/values/5
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE api/values/5
        public void Delete(int id)
        {
        }
    }
}
