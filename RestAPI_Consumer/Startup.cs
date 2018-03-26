using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Owin;
using Owin;
using RabbitMQ.Client;
using RabbitMQFactory;
using RabbitMQ.Client.Events;
using System.IO;
using System.Text;

[assembly: OwinStartup(typeof(RestAPI_Consumer.Startup))]

namespace RestAPI_Consumer
{
    public partial class Startup
    {
        private const string WORK_EXCHANGE = "SendMessages";
        private const string WORK_QUEUE = "A";


        private const string RETRY_EXCHANGE = "RetryExchange";
        private const string RETRY_QUEUE = "RetryQueue";
        private const int RETRY_DELAY = 60000; // in m
        RabbitMQConnectionFactory rmq = null;
        public void Configuration(IAppBuilder app)
        {
            ConfigureAuth(app);
            RabbitMQConfiguration();
        }

        private void RabbitMQConfiguration()
        {
             rmq = RabbitMQConnectionFactory.RabbitMQConnectionFactoryInstance;
            WorkQueue();
            RetryQueue();
        }


        private void WorkQueue()
        {
            rmq.CreateFanoutExchange(WORK_EXCHANGE, true, false);


            var queueArgs = new Dictionary<string, object> {
          { "x-dead-letter-exchange", RETRY_EXCHANGE }, { "x-message-ttl", RETRY_DELAY }
           };

            rmq.CreateQueue(WORK_QUEUE, true, false, queueArgs);
            rmq.BindQueue(WORK_QUEUE, WORK_EXCHANGE, "");
            try
            {
                var consumer = rmq.ConsumerEventHandlers();
                consumer.Received += Consumer_Received;
                rmq.Consume(WORK_QUEUE, false, consumer);
            }
            catch (Exception ex)
            {

            }
        }
        private void RetryQueue()
        {


            var args = new Dictionary<string, object>
            { { "x-delayed-type", "fanout" } };
            rmq.CreateDelayedExchange(RETRY_EXCHANGE, true, false, args);
            //rmq.CreateFanoutExchange(RETRY_EXCHANGE, true, false);

            rmq.CreateQueue(RETRY_QUEUE, true, false, null);
            rmq.BindQueue(RETRY_QUEUE, RETRY_EXCHANGE, "");

            try
            {
                var consumer = rmq.ConsumerEventHandlers();
                consumer.Received += Consumer_Received1;
                rmq.Consume(RETRY_QUEUE, false, consumer);
            }
            catch (Exception ex)
            {

            }

        }

        private void Consumer_Received(object sender, BasicDeliverEventArgs e)
        {

            try
            {
                var body = e.Body;
                var content = Encoding.UTF8.GetString(body);
                using (StreamWriter sw = new StreamWriter(@"C:\wahid off\test.txt", true))
                {
                    sw.WriteLine(content + " " + DateTime.Now);
                }
                //Instance.ConsumerAcknowledgements().BasicAck(e.DeliveryTag, false);
                if (e.BasicProperties.Headers != null)
                {
                    var header = e.BasicProperties.Headers;
                    int redeliveredCount = Convert.ToInt32(header["X-delivered-Count"]);
                    if (redeliveredCount <= 1)
                    {
                        e.BasicProperties.Headers["X-delivered-Count"] = redeliveredCount++;
                        rmq.ConsumerAcknowledgements().BasicAck(e.DeliveryTag, false);
                        Redeliver(e.Body, redeliveredCount);
                    }
                    else
                    {
                        rmq.ConsumerAcknowledgements().BasicReject(e.DeliveryTag, false);
                    }


                }
                else
                {
                    rmq.ConsumerAcknowledgements().BasicReject(e.DeliveryTag, false);
                }

                using (StreamWriter sw = new StreamWriter(@"C:\wahid off\test1.txt", true))
                {
                    sw.WriteLine("redeL : " + e.Redelivered);
                    sw.WriteLine(e.DeliveryTag);
                    sw.WriteLine(Convert.ToInt32(e.BasicProperties.Headers["X-delivered-Count"]));
                }
            }
            catch (Exception ex)
            {


                using (StreamWriter sw = new StreamWriter(@"C:\wahid off\testExce.txt", true))
                {
                    sw.WriteLine(ex.ToString());
                }
            }
        }

        private void Consumer_Received1(object sender, BasicDeliverEventArgs e)
        {
            try
            {
                var body = e.Body;
                var content = Encoding.UTF8.GetString(body);
                using (StreamWriter sw = new StreamWriter(@"C:\wahid off\test.txt", true))
                {
                    sw.WriteLine("DLX " + content + " " + DateTime.Now);
                }
                rmq.ConsumerAcknowledgements().BasicReject(e.DeliveryTag, false);
                using (StreamWriter sw = new StreamWriter(@"C:\wahid off\test1.txt", true))
                {
                    sw.WriteLine("DLX redeL : " + e.Redelivered);
                    sw.WriteLine(e.DeliveryTag);
                    sw.WriteLine(Convert.ToInt32(e.BasicProperties.Headers["X-delivered-Count"]));
                }
            }
            catch (Exception ex)
            {


                using (StreamWriter sw = new StreamWriter(@"C:\wahid off\testExce.txt", true))
                {
                    sw.WriteLine(ex.ToString());
                }
            }
        }

        private void Redeliver(byte[] message, int redeliveredCount)
        {
           

            rmq.CreateFanoutExchange("SendMessages", true, false);
            bool isPublished = rmq.PublishMessage("SendMessages", "", false, null, message, redeliveredCount);
        }
    }
}
