using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQFactory;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BackGroundJobProcessor
{
    public partial class EmailProcessor : ServiceBase
    {
        RabbitMQConnectionFactory Instance;

        private const string WORK_EXCHANGE = "SendMessages";
        private const string WORK_QUEUE = "A";


        private const string RETRY_EXCHANGE = "RetryExchange";
        private const string RETRY_QUEUE = "RetryQueue";
        private const int RETRY_DELAY = 10000; // in m


        
        public EmailProcessor()
        {
            InitializeComponent();
            Instance = RabbitMQConnectionFactory.RabbitMQConnectionFactoryInstance;
        }


        protected override void OnStart(string[] args)
        {
            Thread.Sleep(20000);
            WorkQueue();
            RetryQueue();
        }

        private void WorkQueue()
        {
            Instance.CreateFanoutExchange(WORK_EXCHANGE, true, false);


            var queueArgs = new Dictionary<string, object> {
          { "x-dead-letter-exchange", RETRY_EXCHANGE }
           };

            Instance.CreateQueue(WORK_QUEUE, true, false, queueArgs);
            Instance.BindQueue(WORK_QUEUE, WORK_EXCHANGE, "");
            try
            {
                var consumer = Instance.ConsumerEventHandlers();
                consumer.Received += Consumer_Received;
                Instance.Consume(WORK_QUEUE, false, consumer);
            }
            catch (Exception ex)
            {

            }
        }
        private void RetryQueue()
        {

            var queueArgs = new Dictionary<string, object> {
           { "x-message-ttl", RETRY_DELAY }};

            Instance.CreateFanoutExchange(RETRY_EXCHANGE, true,false);

            Instance.CreateQueue(RETRY_QUEUE, true, false, null);
            Instance.BindQueue(RETRY_QUEUE, RETRY_EXCHANGE, "");

            try
            {
                var consumer = Instance.ConsumerEventHandlers();
                consumer.Received += Consumer_Received1;
                Instance.Consume(RETRY_QUEUE, false, consumer);
            }
            catch (Exception ex)
            {

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
                    sw.WriteLine("DLX " + content);
                }
                Instance.ConsumerAcknowledgements().BasicReject(e.DeliveryTag, false);
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

        private void Consumer_Received(object sender, BasicDeliverEventArgs e)
        {
            try
            {
                var body = e.Body;
                var content = Encoding.UTF8.GetString(body);
                using (StreamWriter sw = new StreamWriter(@"C:\wahid off\test.txt", true))
                {
                    sw.WriteLine(content);
                }

                //Instance.ConsumerAcknowledgements().BasicAck(e.DeliveryTag, false);
                if (e.BasicProperties.Headers != null)
                {
                        var header = e.BasicProperties.Headers;
                        int redeliveredCount = Convert.ToInt32(header["X-delivered-Count"]);
                        if (redeliveredCount <= 1)
                        {
                            e.BasicProperties.Headers["X-delivered-Count"] = redeliveredCount++;
                            Instance.ConsumerAcknowledgements().BasicAck(e.DeliveryTag, true);
                            Redeliver(e.Body, redeliveredCount);
                        }
                        else
                        {
                            Instance.ConsumerAcknowledgements().BasicNack(e.DeliveryTag, false,false);
                        }
                    

                }
                else
                {
                    Instance.ConsumerAcknowledgements().BasicReject(e.DeliveryTag, false);
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
           

        private void Redeliver(byte[] message,int redeliveredCount)
        {
            RabbitMQConnectionFactory Instance = RabbitMQConnectionFactory.RabbitMQConnectionFactoryInstance;

            Instance.CreateFanoutExchange("SendMessages", true, false);
           bool isPublished= Instance.PublishMessage("SendMessages", "", false, null, message,redeliveredCount);
        }
        

        protected override void OnStop()
        {

            try
            {
                Instance.Close();
            }
            catch (IOException ex)
            {
                // Close() may throw an IOException if connection
                // dies - but that's ok (handled by reconnect)
            }
        }


    }
}
