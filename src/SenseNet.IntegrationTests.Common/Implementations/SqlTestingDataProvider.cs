﻿//using System;
//using System.Collections.Generic;
//using System.Data;
//using Newtonsoft.Json;
//using SenseNet.ContentRepository.Storage.Data;
//using SenseNet.ContentRepository.Storage.Data.SqlClient;
//using SenseNet.Diagnostics;
//using SenseNet.Tests.Implementations;

//namespace SenseNet.IntegrationTests.Common.Implementations
//{
//    public class SqlTestingDataProvider : ITestingDataProviderExtension
//    {

//        // Use DataStore.DataProvider instead of DataProvider.Instance
//        // Use and extend MsSqlTools

//        public DataProvider MainProvider { get; set; }
//        public void InitializeForTests()
//        {
//            using (var proc = DataProvider.Instance.CreateDataProcedure(@"
//ALTER TABLE [BinaryProperties] CHECK CONSTRAINT ALL
//ALTER TABLE [FlatProperties] CHECK CONSTRAINT ALL
//ALTER TABLE [Nodes] CHECK CONSTRAINT ALL
//ALTER TABLE [ReferenceProperties] CHECK CONSTRAINT ALL
//ALTER TABLE [TextPropertiesNText] CHECK CONSTRAINT ALL
//ALTER TABLE [TextPropertiesNVarchar] CHECK CONSTRAINT ALL
//ALTER TABLE [Versions] CHECK CONSTRAINT ALL
//"))
//            {
//                proc.CommandType = CommandType.Text;
//                proc.ExecuteNonQuery();
//            }
//        }

//        public string GetSecurityControlStringForTests()
//        {
//            var securityEntitiesArray = new List<object>();
//            using (var cmd = new SqlProcedure { CommandText = "SELECT NodeId, ParentNodeId, [OwnerId] FROM Nodes ORDER BY NodeId", CommandType = CommandType.Text })
//            {
//                using (var reader = cmd.ExecuteReader())
//                {
//                    int count = 0;
//                    while (reader.Read())
//                    {
//                        securityEntitiesArray.Add(new
//                        {
//                            NodeId = reader.GetSafeInt32(0),
//                            ParentId = reader.GetSafeInt32(1),
//                            OwnerId = reader.GetSafeInt32(2),
//                        });

//                        count++;
//                        // it is neccessary to examine the number of Nodes, because loading a too big security structure may require too much resource
//                        if (count > 200000)
//                            throw new ArgumentOutOfRangeException("number of Nodes");
//                    }
//                }
//            }
//            return JsonConvert.SerializeObject(securityEntitiesArray);
//        }

//        public int GetPermissionLogEntriesCountAfterMoment(DateTime moment)
//        {
//            var count = 0;
//            var sql =
//                $"SELECT COUNT(1) FROM LogEntries WHERE Title = 'PermissionChanged' AND LogDate>='{moment.ToString("yyyy-MM-dd HH:mm:ss")}'";
//            var proc = DataProvider.Instance.CreateDataProcedure(sql);
//            proc.CommandType = System.Data.CommandType.Text;
//            using (var reader = proc.ExecuteReader())
//            {
//                while (reader.Read())
//                {
//                    count = reader.GetSafeInt32(0);
//                }
//            }
//            return count;
//        }

//        public AuditLogEntry[] LoadLastAuditLogEntries(int count)
//        {
//            throw new NotImplementedException();
//        }

//        public void CheckScript(string commandText)
//        {
//            // c:\Program Files\Microsoft SQL Server\90\SDK\Assemblies\Microsoft.SqlServer.Smo.dll
//            // c:\Program Files\Microsoft SQL Server\90\SDK\Assemblies\Microsoft.SqlServer.ConnectionInfo.dll

//            // The code maybe equivalent to this script:
//            // SET NOEXEC ON
//            // GO
//            // SELECT * FROM Nodes
//            // GO
//            // SET NOEXEC OFF
//            // GO

//            throw new NotImplementedException();
//        }

//        public IEnumerable<IndexIntegrityCheckerItem> GetTimestampDataForOneNodeIntegrityCheck(string path, int[] excludedNodeTypeIds)
//        {
//            throw new NotImplementedException();
//        }

//        public IEnumerable<IndexIntegrityCheckerItem> GetTimestampDataForRecursiveIntegrityCheck(string path, int[] excludedNodeTypeIds)
//        {
//            throw new NotImplementedException();
//        }

//    }
//}
