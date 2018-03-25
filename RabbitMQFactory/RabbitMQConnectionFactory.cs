using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQFactory
{

    public class RabbitMQConnectionFactory
    {
        static RabbitMQConnectionFactory rabbitMQConnectionFactory = null;
        IConnection connection;
        IModel channel;

        private static readonly object Locker = new object();

       private RabbitMQConnectionFactory()
        {
            var connectionFactory = new ConnectionFactory
            {
                HostName = "localhost",

                UserName = "guest",
                Password = "guest",
                VirtualHost = "/"
            };
            connection = connectionFactory.CreateConnection();
            channel = connection.CreateModel();
        }


        public static RabbitMQConnectionFactory RabbitMQConnectionFactoryInstance
        {
            get
            {
                if (rabbitMQConnectionFactory == null)
                {
                    lock (Locker)
                    {
                        if (rabbitMQConnectionFactory == null)
                        {
                            rabbitMQConnectionFactory = new RabbitMQConnectionFactory();
                        }
                    }
                }
                return rabbitMQConnectionFactory;
            }
        }


        #region Exchange
        public IModel CreateDirectExchange(string exchangeName, bool durable, bool autoDelete)
        {

            channel.ExchangeDeclare(exchangeName, ExchangeType.Direct, durable, autoDelete, null);
            return channel;
        }

        public IModel CreateFanoutExchange(string exchangeName, bool durable, bool autoDelete)
        {

            channel.ExchangeDeclare(exchangeName, ExchangeType.Fanout, durable, autoDelete, null);
            return channel;
        }

        public IModel CreateDelayedExchange(string exchangeName, bool durable, bool autoDelete,Dictionary<string, object> queueArgs)
        {

            channel.ExchangeDeclare(exchangeName, "x-delayed-message", durable, autoDelete, queueArgs);
            return channel;
        }
        #endregion

        public bool CreateQueue(string queueName, bool durable, bool autoDelete, Dictionary<string, object> queueArgs)
        {
            try
            {
                channel.QueueDeclare(queueName, durable, false, autoDelete, queueArgs);
            }
            catch (Exception ex)
            {
                return false;
            }
            return true;
        }

        public bool BindQueue(string queueName, string exchangeName, string routeKey)
        {
            try
            {
                channel.QueueBind(queueName, exchangeName, routeKey);
            }
            catch (Exception ex)
            {
                return false;
            }
            return true;
        }

        public bool PublishMessage(string exchangeName, string RoutingKey, bool mandatory, IBasicProperties basicProperties, byte[] message,int redeliveredCount)
        {
            try
            {
                IBasicProperties properties = channel.CreateBasicProperties();
                properties.DeliveryMode = 2;
                properties.Headers = new Dictionary<string, object>();
                properties.Headers.Add("X-delivered-Count", redeliveredCount);
                channel.BasicPublish(exchangeName, "", properties, message);
               
            }
            catch (Exception ex)
            {
                return false;
            }
            return true;
        }

        
        public EventingBasicConsumer ConsumerEventHandlers()
        {
            var consumer = new EventingBasicConsumer(channel);
            return consumer;
        }

        public bool Consume(string queueName, bool autoAck, IBasicConsumer consumer)
        {
            try
            {
                channel.BasicConsume(queueName, autoAck, consumer);
               
            }
            catch(Exception e)
            {
                return false;
            }
            return true;

        }


        public IModel ConsumerAcknowledgements()
        {
            return channel;
        }
        public void  Close()
        {
            try
            {
                if (channel != null && channel.IsOpen)
                {
                    channel.Close();
                    channel = null;
                }

                if (connection != null && connection.IsOpen)
                {
                    connection.Close();
                    connection = null;
                }
            }
            catch (IOException ex)
            {
                // Close() may throw an IOException if connection
                // dies - but that's ok (handled by reconnect)
            }
        }
        

    }
    
}
