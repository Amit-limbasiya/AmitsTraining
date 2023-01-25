using System.Text;
using MySql.Data.MySqlClient;
namespace practice_sqlserver
{

    class Program
    {
        static AutoResetEvent interruptionFlag= new AutoResetEvent(false);
        static bool signal=false;
        static string connectionDetails = @"server=localhost; port=4306; uid=amitml; pwd=Amit@123; database=mydatabase; charset=utf8; sslMode=none";
        static MySqlConnection conn = new MySqlConnection(connectionDetails);
        static MySqlCommand cmd = conn.CreateCommand();

        static bool working=true;

        static Dictionary<string, bool> parameters = new Dictionary<string, bool>()
        { { "stop",false } , { "status" , false } };
        static StringBuilder sb;
        static List<string> list;

        public static int DoSum(int a, int b)
        {
            Thread.Sleep(50);
            return a + b;
        }

        static void TakeUserRange(out int begin,out int end)      
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

            int begin, end;
            TakeUserRange(out begin, out end);
            Thread migrationThread = new Thread(() => Migration_main(begin, end));
            migrationThread.Start();

            Thread InputThread= new Thread(ConsoleInput);
            InputThread.Start();
        }

        private static void ConsoleInput()
        {
            while (working)
            {
                if (Console.KeyAvailable)
                {
                    signal = true;
                    string order = Console.ReadLine();
                    if (order.Equals("Cancel"))
                    {
                        parameters["stop"] = true;
                    }
                    else if (order.Equals("Status"))
                    {
                        parameters["status"] = true;
                    }
                    signal = false;
                    interruptionFlag.Set();
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
           
            for (int i = 0; i < 10000; i++)
            {
                sb = new StringBuilder("insert into sourcetable (id,number1,number2) values ");
                list = new List<string>();
                for (int j = 1; j <= 100; j++)
                {
                    list.Add(string.Format("({0},{1},{2})", id, random.Next(1, 20), random.Next(1, 20)));
                    id++;
                }
                sb.Append(string.Join(',', list));
                cmd = conn.CreateCommand();
                cmd.CommandText = sb.ToString();
                cmd.ExecuteNonQuery();
            }
            return true;
        }

        public static int WriteInDestinationTable(int begin, int end, List<string> list,int totalcount)
        {
            int k, i = 0;
            sb = new StringBuilder("insert into destinationtable (id,foreignID,sum) values");

            MySqlConnection writeConn = new MySqlConnection(connectionDetails);
            writeConn.Open();
            MySqlCommand writeCmd = writeConn.CreateCommand();

            for (i = begin; i < end; i++)
            {
                if (signal)
                {
                    interruptionFlag.WaitOne();
                }
                if (parameters["stop"]) break;
                sb.Append(list[i] + ',');

                if (parameters["status"])
                {
                    Console.WriteLine(i + " record are inserted till now...");
                    Console.WriteLine((totalcount - i) + " record are Remaining to migrate...");
                    parameters["status"] = false;
                }
            }

            if (i != begin)
            {
                string commandString = sb.ToString();
                commandString = commandString.Remove(commandString.Length - 1, 1);
                writeCmd.CommandText = commandString;
                writeCmd.ExecuteNonQuery();

            }

            if (parameters["stop"])
                Console.WriteLine("Migration Cancelled...");
            else
                Console.WriteLine("Migration chunk done...");
            writeConn.Close();
            return i;
        }
        public static List<string> Migrate(int begin,int end,int id)
        {
            cmd.CommandText = string.Format("select * from sourcetable where id between {0} and {1}", begin, end);
            MySqlDataReader reader = cmd.ExecuteReader();
            int counter = 0;
            int totalcount = end - begin;
            Console.WriteLine(totalcount);
            list = new List<string>();
            if (reader.HasRows)
            {
                int sum = 0;
                while (reader.Read())
                {
                    if (counter % 100==0 && counter!=0)
                    {
                        WriteInDestinationTable(counter-100,counter,list,totalcount);
                    }
                    if (parameters["stop"]) break;
                    
                    sum = DoSum(reader.GetInt32(1), reader.GetInt32(2));
                    list.Add(string.Format("({0},{1},{2})", id, reader.GetInt32(0), sum));
                    id++;
                    counter++;
                }
                if (counter % 100 != 0 && !parameters["stop"])
                {
                    int rbegin=counter-(counter%100);
                    WriteInDestinationTable(rbegin,counter,list,totalcount);
                }
                reader.Close();
            }
            else
            {
                Console.WriteLine("No rows found...");
            }
            return list;
        }
        public static void Migration_main(int begin, int end)
        {
            System.Console.WriteLine("connected...");
            int id = 1;
            using (conn)
            {
                try
                {
                    conn.Open();
                    TruncateTable("sourcetable");

                    if (AddNumbersToSourceTable())
                        Console.WriteLine("Source table filled...");


                    TruncateTable("destinationtable");
                    
                    while (true)
                    {
                        list=Migrate(begin, end, id);
                        Console.WriteLine(list.Count+" records migrated...");
                        Console.WriteLine("Do you want next iteration? [Y/N]: ");
                        string inp = Console.ReadLine();
                        parameters["stop"] = false;
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
                            working = false;
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
                    Console.WriteLine("press any key to exit...");
                    conn.Close();
                }
            }
        }
    }
}