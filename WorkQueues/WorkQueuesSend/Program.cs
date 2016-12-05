using System;
using System.Text;
using RabbitMQ.Client;

namespace WorkQueuesSend
{
   /// <summary>
   ///    WorkQueuesSend project is modified in this version.
   ///   By setting the property of queue durable to true and the message property to persist, we are making sure that
   ///   the message doesn't get lost if the consumer goes down without sending the acknowledgment.
   /// </summary>
   internal class Program
   {
      private static void Main(string[] args)
      {
         var factory = new ConnectionFactory {HostName = "localhost"};
         using (var connection = factory.CreateConnection())
         using (var channel = connection.CreateModel())
         {
            //channel.QueueDeclare("task_queue", false, false, false, null);
            channel.QueueDeclare(queue: "task_queue",
               durable: true,
               exclusive: false,
               autoDelete: false,
               arguments: null);

            var message = GetMessage(args);
            for (int x = 1; x <= 50; x++)
            {
               var body = Encoding.UTF8.GetBytes(message + " = " + x + "..");

               var properties = channel.CreateBasicProperties();
               properties.Persistent = true;

               channel.BasicPublish(exchange: "",
                  routingKey: "task_queue",
                  basicProperties: properties,
                  body: body);
            }
            Console.WriteLine(" [x] Sent {0}", message);
         }

         Console.WriteLine(" Press [enter] to exit.");
         Console.ReadLine();
      }

      //returns the message passed through args. If args is null, returns default message.
      private static string GetMessage(string[] args)
      {
         return args.Length > 0 ? string.Join(" ", args) : "Hello World!";
      }
   }
}