using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;

namespace SenseNet.IntegrationTests.Common.Implementations
{
    public class MsSqlTools
    {
        public static void ExecuteNonQuery(string sql, string connectionString = null)
        {
            ExecuteNonQueryAsync(sql, connectionString).ConfigureAwait(false).GetAwaiter().GetResult();
        }
        public static async Task ExecuteNonQueryAsync(string sql, string connectionString = null,
            CancellationToken cancellationToken = default)
        {
            Configuration.ConnectionStrings.ConnectionString = SenseNet.IntegrationTests.Common.ConnectionStrings.ForPackagingTests;
            using (var ctx = new MsSqlDataContext(cancellationToken))
            {
                await ctx.ExecuteNonQueryAsync(sql).ConfigureAwait(false);
            }
        }
    }
}
