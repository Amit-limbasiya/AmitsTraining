// we can notify another thread or we can pass the data to another thread so that by that information we can put the inputtaker thread into a deep sleep  
//and we can notify after taking input for migration thread.
using System;
using System.Data;
using System.Globalization;
using System.Text;
using MySql.Data.MySqlClient;
namespace practice_sqlserver
{
    class Program
    {
        static string connectionDetails = @"server=localhost; port=4306; uid=amitml; pwd=Amit@123; database=mydatabase; charset=utf8; sslMode=none";
        static MySqlConnection conn = new MySqlConnection(connectionDetails);
        static MySqlCommand cmd = conn.CreateCommand();

        static Dictionary<string, bool> parameters = new Dictionary<string, bool>()
        { { "stop",false } , { "status" , false } };
        static StringBuilder sb;
        static List<string> list;

        public static int DoSum(int a, int b)
        {
            Thread.Sleep(50);
            return a + b;
        }

        static void TakeUserRange(out int begin,out int end)      //taking user input for beginning and end of the input.
        {
            bool validbegin = false;
            bool validend = false;
            Console.WriteLine("Enter the beginning: ");
            validbegin = int.TryParse(Console.ReadLine(), out begin);
            Console.WriteLine("Enter the end: ");
            validend = int.TryParse(Console.ReadLine(), out end);


            while ((!validbegin || !validend || begin > end || begin <= 0 || end > 1000000))
            {
                Console.WriteLine("Enter appropriate range or value...");
                Console.WriteLine("Enter the beginning: ");
                validbegin = int.TryParse(Console.ReadLine(), out begin);
                Console.WriteLine("Enter the end: ");
                validend = int.TryParse(Console.ReadLine(), out end);
            }

        }


        static void Main(string[] args)
        {
            //taking user input for beginning and end of the input.
           
            int begin, end;
            TakeUserRange(out begin,out end);

            //Thread inptakerThread = new Thread(InputTaker);
            Thread migrationThread = new Thread(() => Migration_main(begin, end));
            migrationThread.Start();

            //inptakerThread.Start();
            //inptakerThread.Join();
            while (true)
            {
                string order=Console.ReadLine();
                if (order.Equals("Cancel"))
                {
                    parameters["stop"] = true;
                    migrationThread.Join();
                    break;
                }
                else if (order.Equals("Status"))
                {
                    parameters["status"] = true;
                }
            }

        }

        public static void TruncateTable(string tableName)
        {
            cmd.CommandType = System.Data.CommandType.Text;
            cmd.CommandText = "Truncate table "+tableName;
            cmd.ExecuteNonQuery();
        }
        public static bool AddNumbersToSourceTable()
        {
            Random random = new Random();
            int id = 1;
            //for (i = 0; i < 10000 && !parameters["stop"]; i++)  // if we write it then when it will stop it will do i++ unneccessary

            for (int i = 0; i < 10000; i++)
            {
                sb = new StringBuilder("insert into sourcetable (id,number1,number2) values ");
                list = new List<string>();
                for (int j = 1; j <= 100; j++)
                {
                    //list.Add(string.Format("({0},{1},{2})", id, j, 100 - j));
                    list.Add(string.Format("({0},{1},{2})", id, random.Next(1, 20), random.Next(1, 20)));
                    id++;
                }
                sb.Append(string.Join(',', list));
                cmd = conn.CreateCommand();
                //cmd.CommandType = System.Data.CommandType.Text;
                cmd.CommandText = sb.ToString();
                cmd.ExecuteNonQuery();
            }
            return true;
        }

        public static int Migration(int begin, int end,List<string> list)
        {
            int k, i = 0;
            int iterations = (int)Math.Ceiling((end - begin) / 100m);
            for (k = 0; k < iterations; k++)
            {
                sb = new StringBuilder("insert into destinationtable (id,foreignID,sum) values");
                int recentbegin = k * 100;

                int recentend = list.Count < (recentbegin + 100) ? list.Count : recentbegin + 100;

                for (i = recentbegin; i < recentend; i++)
                {
                    if (parameters["stop"]) break;
                    sb.Append(list[i] + ',');

                    if (parameters["status"])
                    {
                        Console.WriteLine(i + " record are inserted till now...");
                        Console.WriteLine((list.Count - i) + " record are Remaining to migrate...");
                        parameters["status"] = false;
                    }
                }

                if (i != recentbegin)
                {
                    string commandString = sb.ToString();
                    commandString = commandString.Remove(commandString.Length - 1, 1);
                    cmd.CommandText = commandString;
                    cmd.ExecuteNonQuery();
                    Thread.Sleep(5000);
                    Console.WriteLine("iteration: " + k);
                }

                if (parameters["stop"])
                {
                    Console.WriteLine("Migration Cancelled...");
                    break;
                }
            }
            return i;
        }
        public static List<string> ReadingRecords(int begin,int end,out bool isRowsAvailable,int id)
        {
            cmd.CommandText = string.Format("select * from sourcetable where id between {0} and {1}", begin, end);
            MySqlDataReader reader = cmd.ExecuteReader();

            list = new List<string>();
            if (reader.HasRows)
            {
                int sum = 0;
                while (reader.Read())
                {
                    if (parameters["stop"]) break;
                    sum = DoSum(reader.GetInt32(1), reader.GetInt32(2));
                    list.Add(string.Format("({0},{1},{2})", id, reader.GetInt32(0), sum));
                    id++;
                }
                reader.Close();
                isRowsAvailable = true;
            }
            else
            {
                Console.WriteLine("No rows found...");
                isRowsAvailable= false;
            }
            return list;
        }
        public static void Migration_main(int begin, int end)
        {
            System.Console.WriteLine("connected...");
            bool isRowsAvailable;
            int id = 1;
            //Console.WriteLine(conn);
            using (conn)
            {
                try
                {
                    conn.Open();
                    //truncate the table first
                    //TruncateTable("sourcetable");

                    //adding numbers into the database
                    //if (AddNumbersToSourceTable())
                    //    Console.WriteLine("Source table filled...");


                    //truncate destination table
                    TruncateTable("destinationtable");
                    
                    //Migration: add sum into the destination table
                    while (true)
                    {
                        list=ReadingRecords(begin, end, out isRowsAvailable, id);
                        if (isRowsAvailable)
                        {
                            int num_of_records = Migration(begin, end, list);
                            Console.WriteLine(num_of_records + " records migrated successfully...");
                        }
                        else
                        {
                            Console.WriteLine("No rows Found...");
                        }
                        
                        Console.WriteLine("Do you want next iteration? first enter no and then [Y/N]: ");
                        Thread.Yield();
                        string inp = Console.ReadLine();
                        while (inp.Length != 1 || (!inp.Equals("Y") && !inp.Equals("N")))
                        {
                            Console.WriteLine("invalid input..please enter no and then Y/N: ");
                            inp = Console.ReadLine();
                        }
                        if (inp.Equals("Y"))
                        {
                            TakeUserRange(out begin, out end);
                            id+=list.Count;
                        }
                        else
                        {
                            break;
                        }
                        
                    }
                    
                }
                catch (System.FormatException)
                {
                    Console.WriteLine("You entered inappropreate value.");
                }
                catch (MySql.Data.MySqlClient.MySqlException e)
                {
                    System.Console.WriteLine("error block");
                    System.Console.WriteLine(e.Message.ToString());
                }
                finally
                {
                    System.Console.WriteLine("Successful connection and in finally block...");
                    conn.Close();
                }
            }
        }
    }
}