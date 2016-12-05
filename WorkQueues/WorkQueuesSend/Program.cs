using System;
using System.Text;
using RabbitMQ.Client;

namespace WorkQueuesSend
{
   /// <summary>
   ///    Hello World project has been modified here. User can send the message as a parameter.
   ///    The number of .(dots) at the end of the message determines the number of seconds the threat has to put to sleep.
   ///   The queue name and the routing key name should be the same since we are using the default exchange.
   /// </summary>
   internal class Program
   {
      private static void Main(string[] args)
      {
         var factory = new ConnectionFactory {HostName = "localhost"};
         using (var connection = factory.CreateConnection())
         using (var channel = connection.CreateModel())
         {
            channel.QueueDeclare("task_queue", false, false, false, null);

            var message = GetMessage(args);
            for (int x = 1; x <= 50; x++)
            {
               var body = Encoding.UTF8.GetBytes(message + " = " + x + "..");

               var properties = channel.CreateBasicProperties();
               properties.Persistent = true;

               channel.BasicPublish("", "task_queue", properties, body);
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