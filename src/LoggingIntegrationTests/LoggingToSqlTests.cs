using System.Linq;
using System.Threading;
using LoggingIntegrationTests.Implementations;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Configuration;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;
using SenseNet.Diagnostics;

namespace LoggingIntegrationTests
{
    [TestClass]
    public class LoggingToSqlTests : LoggerTestBase
    {
        private MsSqlDataProvider Dp
        {
            get
            {
                if (DataStore.DataProvider is MsSqlDataProvider dp && ConnectionStrings.ConnectionString == SenseNet.IntegrationTests.Common.ConnectionStrings.ForLoggingTests)
                    return dp;
                ConnectionStrings.ConnectionString = SenseNet.IntegrationTests.Common.ConnectionStrings.ForLoggingTests;
                Providers.Instance.DataProvider = (dp = new MsSqlDataProvider());
                return dp;
            }
        }

        [TestMethod]
        public void Logging_Audit_ToSql_ViaDirectAuditWriter()
        {
            InitializeLogEntriesTable(Dp);

            SnLog.AuditEventWriter = new DatabaseAuditEventWriter();

            var testMessage = "Msg1";

            // action
            SnLog.WriteAudit(new TestAuditEvent(testMessage));

            // assert
            var auditEvent = Dp.LoadLastAuditEventsAsync(1, CancellationToken.None).GetAwaiter().GetResult()
                .FirstOrDefault();
            Assert.IsNotNull(auditEvent);
            Assert.AreEqual(testMessage, auditEvent.Message);
        }
    }
}
