using System;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace WorkQueuesReceive
{
   /// <summary>
   /// Using message acknowledgments and BasicQos you can set up a work queue. 
   /// The durability options let the tasks survive even if RabbitMQ is restarted.
   /// 
   /// We have set the durable property to true. Also the prefetchCount property is set to 1 in the BasicQos method.
   /// This tells RabbitMQ not to give more than one message to a worker at a time. Or, in other words, 
   /// don't dispatch a new message to a worker until it has processed and acknowledged the previous one. 
   /// Instead, it will dispatch it to the next worker that is not still busy.
   /// 
   /// Also the noAck: false makes sure that an acknowledgement is sent back from the consumer to tell RabbitMQ 
   /// that a particular message has been received, processed and that RabbitMQ is free to delete it.
   /// </summary>
   internal class Program
   {
      private static void Main(string[] args)
      {
         var factory = new ConnectionFactory {HostName = "localhost"};
         using (var connection = factory.CreateConnection())
         using (var channel = connection.CreateModel())
         {
            //channel.QueueDeclare("hello", false, false, false, null);
            channel.QueueDeclare(queue: "task_queue",
               durable: true,
               exclusive: false,
               autoDelete: false,
               arguments: null);

            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
            Console.WriteLine(" [*] Waiting for messages.");

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
               var body = ea.Body;
               var message = Encoding.UTF8.GetString(body);
               Console.WriteLine(" [x] Received {0}", message);

               var dots = message.Split('.').Length - 1;
               Thread.Sleep(dots*1000);

               Console.WriteLine(" [x] Done");

               channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false); //Required to set acknowlegements.
            };
            channel.BasicConsume(queue: "task_queue", noAck: false, consumer: consumer); // noAck: false will say that acknowledge is a must.

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
         }
      }
   }
}