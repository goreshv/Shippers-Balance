using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;

public class KeyValuePair
{
    public string key { get; set; }
    public string value { get; set; }
}

public class Root
{
    public List<KeyValuePair> key_value_pairs { get; set; }
}

public class Program
{
    public DataTable dbTable = new DataTable();

    const string const_Validation = "Validation";
    public void PullData()
    {
        string connectionString = "Data Source = 3DGLOBALIT-006; Initial Catalog = csv; User = goresh.vashishtha@3dglobalit.com; Password = Goresh@1610";
        string query = "SELECT * FROM shipperscolmapping";

        using (SqlConnection conn = new SqlConnection(connectionString))
        {
            SqlCommand cmd = new SqlCommand(query, conn);
            conn.Open();

            SqlDataAdapter da = new SqlDataAdapter(cmd);
            da.Fill(dbTable);

            conn.Close();
            da.Dispose();
        }
    }

    public static void Main()
    {
        Program program = new Program();
        program.PullData();

        string filePath = "C:\\Users\\GoreshVashishtha\\Desktop\\Shippers Balance\\TMPL Shippers Balance - September.csv";

        ProcessFile(filePath, program);
    }

    public static void ProcessFile(string filePath, Program program)
    {
        try
        {
            string[] files = File.ReadAllLines(filePath);
            List<string> headers = files[0].Split(',').Select(h => h.Trim()).ToList();

            List<Root> rootObjects = new List<Root>();

            for (int i = 1; i < files.Length; i++)
            {
                string[] data = files[i].Split(',');

                Root root = new Root
                {
                    key_value_pairs = new List<KeyValuePair>()
                };

                foreach (var header in headers)
                {
                    KeyValuePair kvp = new KeyValuePair
                    {
                        key = header,
                        value = data[headers.IndexOf(header)].Trim()
                    };

                    root.key_value_pairs.Add(kvp);
                }

                rootObjects.Add(root);
            }

            string connectionString = "Data Source = 3DGLOBALIT-006; Initial Catalog = csv; User = goresh.vashishtha@3dglobalit.com; Password = Goresh@1610";

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();

                DataTable shipBalance = new DataTable();

                foreach (var kvp in program.dbTable.AsEnumerable())
                {
                    string allegroColumnName = kvp.Field<string>("allegrocolumnname");
                    if (!shipBalance.Columns.Contains(allegroColumnName))
                    {
                        //shipBalance.Columns.Add(allegroColumnName, typeof(string));
                        shipBalance.Columns.Add("Pipeline", typeof(string));
                        shipBalance.Columns.Add("Month", typeof(string));
                        shipBalance.Columns.Add("Status", typeof(string));
                        shipBalance.Columns.Add("Updated Date", typeof(string));
                        shipBalance.Columns.Add("Shipper", typeof(string));
                        shipBalance.Columns.Add("Material Name", typeof(string));
                        shipBalance.Columns.Add("Material Code", typeof(string));
                        shipBalance.Columns.Add("Record Type", typeof(string));
                        shipBalance.Columns.Add("Batch Id", typeof(string));
                        shipBalance.Columns.Add("Nomination Month", typeof(string));
                        shipBalance.Columns.Add("Transaction Date", typeof(string));
                        shipBalance.Columns.Add("Custody Location", typeof(string));
                        shipBalance.Columns.Add("Connected Facility", typeof(string));
                        shipBalance.Columns.Add("Volume (m3)", typeof(string));
                        shipBalance.Columns.Add(const_Validation, typeof(string));
                    }
                }

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
                {
                    bulkCopy.DestinationTableName = "shippersbalance_tbl";

                    bulkCopy.ColumnMappings.Add("Pipeline", "pipeline");
                    bulkCopy.ColumnMappings.Add("Month", "month");
                    bulkCopy.ColumnMappings.Add("Status", "status");
                    bulkCopy.ColumnMappings.Add("Updated Date", "updateddate");
                    bulkCopy.ColumnMappings.Add("Shipper", "shipper");
                    bulkCopy.ColumnMappings.Add("Material Name", "materialname");
                    bulkCopy.ColumnMappings.Add("Material Code", "materialcode");
                    bulkCopy.ColumnMappings.Add("Record Type", "recordtype");
                    bulkCopy.ColumnMappings.Add("Batch ID", "batchId");
                    bulkCopy.ColumnMappings.Add("Nomination Month", "nominationmonth");
                    bulkCopy.ColumnMappings.Add("Transaction Date", "transactiondate");
                    bulkCopy.ColumnMappings.Add("Custody Location", "custodylocation");
                    bulkCopy.ColumnMappings.Add("Connected Facility", "connectedfacility");
                    bulkCopy.ColumnMappings.Add("Volume (m3)", "volume");
                    bulkCopy.ColumnMappings.Add(const_Validation, "validation");


                    foreach (var root in rootObjects)
                    {
                        StringBuilder Validation = new StringBuilder();
                        bool atleastOneSuccess = false, atleastOneError = false;
                        Validation.Clear();

                        DataRow newRow = shipBalance.NewRow();

                        foreach (var header in headers)
                        {
                            string value = root.key_value_pairs.FirstOrDefault(x => x.key == header)?.value ?? string.Empty;

                            if (string.IsNullOrEmpty(value))
                            {
                                Validation.Append($"{header} info not received. ");
                                atleastOneError = true;
                            }
                            newRow[header] = value;
                        }

                        newRow[const_Validation] = atleastOneError ? Validation.ToString() : "Success";


                        shipBalance.Rows.Add(newRow);
                    }
                    bulkCopy.WriteToServer(shipBalance);
                }

                shipBalance.Clear();

                conn.Close();
            }

            Console.WriteLine("Data inserted successfully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
    }
}

