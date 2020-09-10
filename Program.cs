using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;

namespace bigLoader
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine(DateTime.Now.ToString() + ": Let's get this party started");

            var fileloc = "C:\\files";
            var processedFileLoc = "C:\\files\\processed";
            var batchSize = 100000;
            var counter = 0;
            Encoding encoding = Encoding.Default;

            DataTable dt = new DataTable();
            dt.Columns.Add(new DataColumn("athlete_id", System.Type.GetType("System.String")));
            dt.Columns.Add(new DataColumn("sub_account", System.Type.GetType("System.String")));
            dt.Columns.Add(new DataColumn("account_type_id", System.Type.GetType("System.Int32")));
            dt.Columns.Add(new DataColumn("created_user", System.Type.GetType("System.String")));
            dt.Columns.Add(new DataColumn("created_dttm", System.Type.GetType("System.DateTime")));
            dt.Columns.Add(new DataColumn("updated_user", System.Type.GetType("System.String")));
            dt.Columns.Add(new DataColumn("updated_dttm", System.Type.GetType("System.DateTime")));

            var processingFile = Directory.GetFiles(fileloc).First();

            //TruncateTable("dbo.gcdb_athlete_id_sub_account_xref");

            //use streamreader to open giant file
            Console.WriteLine(DateTime.Now.ToString() + ": read the file");
            using (StreamReader sr = new StreamReader(processingFile))
            {
                encoding = sr.CurrentEncoding;
                var line = sr.ReadLine();

                while (!sr.EndOfStream)
                {
                    line = sr.ReadLine();

                    var fields = line.Split(',');

                    DataRow dr = dt.NewRow();
                    dr = LoadRecordtoList(dr, fields);
                    dt.Rows.Add(dr.ItemArray);
                    counter += 1;

                    if (dt.Rows.Count == batchSize)
                    {
                        try
                        {
                            Console.WriteLine(DateTime.Now.ToString() + ": Insert a batch of data -- ");

                            loadBulkData(dt, batchSize, "dbo.gcdb_athlete_id_sub_account_xref"); 

                            Console.WriteLine(DateTime.Now.ToString() + ": " + counter + " rows have been processed.");
                            
                            dt = new DataTable();
                            dt.Columns.Add(new DataColumn("athlete_id", System.Type.GetType("System.String")));
                            dt.Columns.Add(new DataColumn("sub_account", System.Type.GetType("System.String")));
                            dt.Columns.Add(new DataColumn("account_type_id", System.Type.GetType("System.Int32")));
                            dt.Columns.Add(new DataColumn("created_user", System.Type.GetType("System.String")));
                            dt.Columns.Add(new DataColumn("created_dttm", System.Type.GetType("System.DateTime")));
                            dt.Columns.Add(new DataColumn("updated_user", System.Type.GetType("System.String")));
                            dt.Columns.Add(new DataColumn("updated_dttm", System.Type.GetType("System.DateTime")));
                        }
                        catch (Exception ex)
                        {

                        }
                    }
                }
            }

            Console.WriteLine(DateTime.Now.ToString() + ": moving the file...");
            File.Move(processingFile, processedFileLoc + "\\" + processingFile.Split('\\').Last());
        }

        private static DataRow LoadRecordtoList(DataRow dr, string[] fields)
        {
            try
            {
                
                dr[0] = fields[0].ToString();
                dr[1] = fields[1].ToString();
                dr[2] = Convert.ToInt32(fields[2].ToString());
                dr[3] = "IDGraphProcessor";
                dr[4] = DateTime.Now.ToString();
                dr[5] = "IDGraphProcessor";
                dr[6] = DateTime.Now.ToString();
                return dr;
            }
            catch (Exception ex) { throw ex; }
        }

        public static void TruncateTable(string tableName)
        {
            using (SqlConnection con_gcdb = new SqlConnection(ConfigurationManager.ConnectionStrings["CRM"].ConnectionString))
            {
                con_gcdb.Open();

                SqlCommand com = new SqlCommand("TRUNCATE TABLE " + tableName, con_gcdb);
                com.ExecuteNonQuery();
                
                con_gcdb.Close();
            }
        }

        //Loads Data into the ETL table in batch sizes.
        public static void loadBulkData(DataTable dt, int batchsize, String tableName)
        {
            using (SqlConnection con_gcdb = new SqlConnection(ConfigurationManager.ConnectionStrings["CRM"].ConnectionString))
            {
                con_gcdb.Open();

                using (SqlBulkCopy sbc = new SqlBulkCopy(con_gcdb))
                {
                    sbc.BulkCopyTimeout = 3600;
                    sbc.DestinationTableName = tableName;
                    sbc.BatchSize = batchsize;
                    sbc.WriteToServer(dt);
                }
                con_gcdb.Close();
            }
        }
    }
}
