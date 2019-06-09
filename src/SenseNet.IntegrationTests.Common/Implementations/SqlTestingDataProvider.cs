using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Newtonsoft.Json;
using SenseNet.Common.Storage.Data;
using SenseNet.ContentRepository.Storage;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.SqlClient;
using SenseNet.ContentRepository.Storage.DataModel;
using SenseNet.ContentRepository.Storage.Schema;
using SenseNet.Diagnostics;
using SenseNet.Tests.Implementations;

namespace SenseNet.IntegrationTests.Common.Implementations
{
    public class SqlTestingDataProvider : ITestingDataProviderExtension
    {
        private DataProvider __mainProvider;
        private DataProvider MainProvider_OLD
        {
            get
            {
                if (DataStore.Enabled)
                    throw new PlatformNotSupportedException();
                return __mainProvider ?? (__mainProvider = DataProvider.Instance);
            }
        }

        private RelationalDataProviderBase __dataProvider;
        private RelationalDataProviderBase MainProvider
        {
            get
            {
                if (!DataStore.Enabled)
                    throw new PlatformNotSupportedException();
                return __dataProvider ?? (__dataProvider = (RelationalDataProviderBase) DataStore.DataProvider);
            }
        }

        public void InitializeForTests()
        {
            using (var proc = MainProvider_OLD.CreateDataProcedure(@"
ALTER TABLE [BinaryProperties] CHECK CONSTRAINT ALL
ALTER TABLE [FlatProperties] CHECK CONSTRAINT ALL
ALTER TABLE [Nodes] CHECK CONSTRAINT ALL
ALTER TABLE [ReferenceProperties] CHECK CONSTRAINT ALL
ALTER TABLE [TextPropertiesNText] CHECK CONSTRAINT ALL
ALTER TABLE [TextPropertiesNVarchar] CHECK CONSTRAINT ALL
ALTER TABLE [Versions] CHECK CONSTRAINT ALL
"))
            {
                proc.CommandType = CommandType.Text;
                proc.ExecuteNonQuery();
            }
        }

        public string GetSecurityControlStringForTests()
        {
            var securityEntitiesArray = new List<object>();
            using (var cmd = new SqlProcedure { CommandText = "SELECT NodeId, ParentNodeId, [OwnerId] FROM Nodes ORDER BY NodeId", CommandType = CommandType.Text })
            {
                using (var reader = cmd.ExecuteReader())
                {
                    int count = 0;
                    while (reader.Read())
                    {
                        securityEntitiesArray.Add(new
                        {
                            NodeId = reader.GetSafeInt32(0),
                            ParentId = reader.GetSafeInt32(1),
                            OwnerId = reader.GetSafeInt32(2),
                        });

                        count++;
                        // it is neccessary to examine the number of Nodes, because loading a too big security structure may require too much resource
                        if (count > 200000)
                            throw new ArgumentOutOfRangeException("number of Nodes");
                    }
                }
            }
            return JsonConvert.SerializeObject(securityEntitiesArray);
        }

        public int GetPermissionLogEntriesCountAfterMoment(DateTime moment)
        {
            var count = 0;
            var sql =
                $"SELECT COUNT(1) FROM LogEntries WHERE Title = 'PermissionChanged' AND LogDate>='{moment.ToString("yyyy-MM-dd HH:mm:ss")}'";
            var proc = MainProvider_OLD.CreateDataProcedure(sql);
            proc.CommandType = System.Data.CommandType.Text;
            using (var reader = proc.ExecuteReader())
            {
                while (reader.Read())
                {
                    count = reader.GetSafeInt32(0);
                }
            }
            return count;
        }

        public AuditLogEntry[] LoadLastAuditLogEntries(int count)
        {
            throw new NotImplementedException();
        }

        public void CheckScript(string commandText)
        {
            // c:\Program Files\Microsoft SQL Server\90\SDK\Assemblies\Microsoft.SqlServer.Smo.dll
            // c:\Program Files\Microsoft SQL Server\90\SDK\Assemblies\Microsoft.SqlServer.ConnectionInfo.dll

            // The code maybe equivalent to this script:
            // SET NOEXEC ON
            // GO
            // SELECT * FROM Nodes
            // GO
            // SET NOEXEC OFF
            // GO

            throw new NotImplementedException();
        }

        public int GetLastNodeId()
        {
            throw new NotImplementedException();
        }

        public void SetContentHandler(string contentTypeName, string handler)
        {
            throw new NotImplementedException();
        }

        public void AddField(string contentTypeName, string fieldName, string fieldType = null, string fieldHandler = null)
        {
            throw new NotImplementedException();
        }

        public Task GetNodeHeadDataAsync(int nodeId)
        {
            throw new NotImplementedException();
        }

        public Task GetVersionDataAsync(int versionId)
        {
            throw new NotImplementedException();
        }

        public async Task<int> GetBinaryPropertyCountAsync(string path)
        {
            using (var ctx = new SnDataContext(MainProvider))
                return (int) await ctx.ExecuteScalarAsync("SELECT COUNT (1) FROM BinaryProperties NOLOCK", cmd => { });
        }

        public async Task<int> GetFileCountAsync(string path)
        {
            using (var ctx = new SnDataContext(MainProvider))
                return (int)await ctx.ExecuteScalarAsync("SELECT COUNT (1) FROM Files NOLOCK", cmd => { });
        }

        public async Task<int> GetLongTextCountAsync(string path)
        {
            using (var ctx = new SnDataContext(MainProvider))
                return (int)await ctx.ExecuteScalarAsync("SELECT COUNT (1) FROM LongTextProperties NOLOCK", cmd => { });
        }

        public async Task<object> GetPropertyValueAsync(int versionId, string name)
        {
            var propertyType = ActiveSchema.PropertyTypes[name];
            if (propertyType == null)
                throw new ArgumentException("Unknown property");

            switch (propertyType.DataType)
            {
                case DataType.Binary:
                    return await GetBinaryPropertyValueAsync(versionId, propertyType);
                case DataType.Text:
                    return await GetLongTextValueAsync(versionId, propertyType);
                default:
                    return await GetDynamicPropertyValueAsync(versionId, propertyType);
            }
        }
        private Task<object> GetBinaryPropertyValueAsync(int versionId, PropertyType propertyType)
        {
            throw new NotImplementedException();
        }
        private async Task<object> GetLongTextValueAsync(int versionId, PropertyType propertyType)
        {
            using (var ctx = new SnDataContext(MainProvider))
            {
                return (string)await ctx.ExecuteScalarAsync(
                    "SELECT TOP 1 Value FROM LongTextProperties " +
                    "WHERE VersionId = @VersionId AND PropertyTypeId = @PropertyTypeId",
                    cmd =>
                    {
                        cmd.Parameters.AddRange(new[]
                        {
                            ctx.CreateParameter("@VersionId", DbType.Int32, versionId),
                            ctx.CreateParameter("@PropertyTypeId", DbType.Int32, propertyType.Id),
                        });
                    });
            }
        }
        private async Task<object> GetDynamicPropertyValueAsync(int versionId, PropertyType propertyType)
        {
            using (var ctx = new SnDataContext(MainProvider))
            {
                var result = (string)await ctx.ExecuteScalarAsync(
                    "SELECT TOP 1 DynamicProperties FROM Versions WHERE VersionId = @VersionId",
                    cmd =>
                    {
                        cmd.Parameters.Add(ctx.CreateParameter("@VersionId", DbType.Int32, versionId));
                    });

                var properties = MainProvider.DeserializeDynamiProperties(result);
                if (properties.TryGetValue(propertyType, out var value))
                    return value;
                return null;
            }
        }


        public Task UpdateDynamicPropertyAsync(int versionId, string name, object value)
        {
            throw new NotImplementedException();
        }

        public Task SetFileStagingAsync(int fileId, bool staging)
        {
            throw new NotImplementedException();
        }

        public Task DeleteFileAsync(int fileId)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IndexIntegrityCheckerItem> GetTimestampDataForOneNodeIntegrityCheck(string path, int[] excludedNodeTypeIds)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IndexIntegrityCheckerItem> GetTimestampDataForRecursiveIntegrityCheck(string path, int[] excludedNodeTypeIds)
        {
            throw new NotImplementedException();
        }

        Task<NodeHeadData> ITestingDataProviderExtension.GetNodeHeadDataAsync(int nodeId)
        {
            throw new NotImplementedException();
        }

        Task<VersionData> ITestingDataProviderExtension.GetVersionDataAsync(int versionId)
        {
            throw new NotImplementedException();
        }
    }
}
