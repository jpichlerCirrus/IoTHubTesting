using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Devices;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace cirrusIQdeviceTwinETL
{
    public class FirmwareVersion
    {
        public string firmwareVersion { get; set; }
    }
    public class configuration
    {
        public string version { get; set; }
    }
    class Program
    {
        static RegistryManager registryManager;
        
        static void Main(string[] args)
        {
            string iothubConnectionString = null;
            if (args[0] == "prod")
            {
                iothubConnectionString = "HostName=CIR-PRD-CUS-IoTHub.azure-devices.net;SharedAccessKeyName=serviceAndRegistryRead;SharedAccessKey=0/aPq49b1ayScssb+U8E2jBhzZGslS1lRac/C+yn2b0=";
            } else
            {
                iothubConnectionString = "HostName=CIR-QA-CUS-IoTHub.azure-devices.net;SharedAccessKeyName=serviceAndRegistryRead;SharedAccessKey=ZknjEPrmtpqYUTSf47l7w8AL8RLL9//kfeLthRbFvyY=";
            }
            
            registryManager = RegistryManager.CreateFromConnectionString(iothubConnectionString);
            performQueryAndPush(args[0]).Wait();
            Console.WriteLine("Process is completed");
            //Console.ReadLine();
            //Console.WriteLine("Hello World!");
        }

        

        public static async Task performQueryAndPush(string environment)
        {
            var query = registryManager.CreateQuery("SELECT * FROM devices", 100);
            string firmwareVersionReported = null;
            string configurationVersionReported = null;
            string firmwareVersionDesired = null;
            string configurationVersionDesired = null;
            string sqlConnectionString = null;
            while (query.HasMoreResults) {
                var page = await query.GetNextAsTwinAsync();
                foreach (var twin in page) {
                    //Console.WriteLine("Twin device ID is: {0}", twin.DeviceId);
                    if (twin.Properties.Reported.Contains("firmwareUpdate")) {
                        var data = JsonConvert.DeserializeObject<FirmwareVersion>(JsonConvert.SerializeObject(twin.Properties.Reported["firmwareUpdate"]));
                        firmwareVersionReported = data.firmwareVersion;
                        //Console.WriteLine("Twin reporting firmware is: {0}", firmwareVersionReported);
                    }
                    if (twin.Properties.Reported.Contains("configuration"))
                    {
                        var data = JsonConvert.DeserializeObject<configuration>(JsonConvert.SerializeObject(twin.Properties.Reported["configuration"]));
                        configurationVersionReported = data.version;
                        //Console.WriteLine("Twin reporting configuration version is: {0}", configurationVersionReported);
                    }
                    if (twin.Properties.Desired.Contains("firmwareUpdate"))
                    {
                        //Console.WriteLine("Twin desired firmwareUpdate is: {0}", JsonConvert.SerializeObject(twin.Properties.Desired["firmwareUpdate"], Formatting.Indented));
                        var data = JsonConvert.DeserializeObject<FirmwareVersion>(JsonConvert.SerializeObject(twin.Properties.Desired["firmwareUpdate"]));
                        firmwareVersionDesired = data.firmwareVersion;
                        //Console.WriteLine("Twin desired firmware is: {0}", firmwareVersionDesired);
                    }
                    if (twin.Properties.Desired.Contains("configuration"))
                    {
                        var data = JsonConvert.DeserializeObject<configuration>(JsonConvert.SerializeObject(twin.Properties.Desired["configuration"]));
                        configurationVersionDesired = data.version;
                        //Console.WriteLine("Twin desired configuration version is: {0}", configurationVersionDesired);
                    }
                    //if (twin.Properties.Reported.Contains("$metadata"))
                    //{
                    //var query2 = registryManager.CreateQuery("SELECT properties.reported.$metadata.configuraiton.version.$lastupdated FROM devices where deviceID = " + twin.DeviceId, 1);
                    //var results = await query2.GetNextAsTwinAsync();
                    //Console.WriteLine(results.Select(t => t.Properties.Reported.GetMetadata)
                    //Console.WriteLine(JsonConvert.SerializeObject(twin.Properties.Reported["$metadata"], Formatting.Indented));
                    //}
                    //Console.WriteLine();
                    //Console.WriteLine();
                    if (environment == "prod") {
                        sqlConnectionString = "Server=tcp:azr-prd-cus-sqls.database.windows.net,1433;Initial Catalog=azr-prd-cus-sqls-cirrusiqops;Persist Security Info=False;User ID=cirrusiqmetrics;" +
                                              "Password=Qb45!z#A9;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
                        
                    } else {
                        sqlConnectionString = "Server=tcp:azr-dev-cus-sqls-wdm.database.windows.net,1433;Initial Catalog=azr-dev-cus-sqls-cirrusiqops;Persist Security Info=False;User ID=cirrusiqmetrics;" +
                                              "Password=Qb45!z#A9;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
                    }
                    using (SqlConnection con = new SqlConnection(sqlConnectionString))
                    {
                        string deleteStatement = "DELETE FROM DeviceTwin where deviceId = '"+twin.DeviceId+"'";
                        using (SqlCommand command = new SqlCommand(deleteStatement, con))
                        {
                            con.Open();
                            int deleteResult = command.ExecuteNonQuery();
                            if (deleteResult < 0)
                                Console.WriteLine("Error with the insert");

                        }
                        string insertStatement = "INSERT INTO DeviceTwin (deviceId, reportedFirmware, reportedConfig, desiredFirmware, desiredConfig) VALUES ('"+twin.DeviceId+"','"+firmwareVersionReported+"','"+configurationVersionReported+"','"+firmwareVersionDesired+"','"+configurationVersionDesired+"')";
                        using (SqlCommand command = new SqlCommand(insertStatement, con))
                        {
                            int insertResult = command.ExecuteNonQuery();
                            if (insertResult < 0)
                                Console.WriteLine("Error with the insert");

                        }
                    }
                }
            }
            //var twinsInHub = await query.GetNextAsTwinAsync();
            //Console.WriteLine("Devices: {0}", string.Join(", ", twinsInHub.Select(t => t.DeviceId)));
        }
    }
}
