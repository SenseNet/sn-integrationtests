using System;
using System.Diagnostics;
using System.Linq;
using LoggingIntegrationTests.Implementations;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Configuration;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;
using SenseNet.Diagnostics;

namespace LoggingIntegrationTests
{
    [TestClass]
    public class EntlibLoggerTests : LoggerTestBase
    {
        private MsSqlDataProvider Dp
        {
            get
            {
                if (DataStore.DataProvider is MsSqlDataProvider dp && ConnectionStrings.ConnectionString == SenseNet.IntegrationTests.Common.ConnectionStrings.ForLoggingTests)
                    return dp;
                ConnectionStrings.ConnectionString = SenseNet.IntegrationTests.Common.ConnectionStrings.ForLoggingTests;
                Providers.Instance.DataProvider2 = (dp = new MsSqlDataProvider());
                return dp;
            }
        }

        [TestMethod]
        public void Logging_Information_ToEntlib()
        {
            using (SwindleLogger(new EntLibLoggerAdapter()))
            {
                InitializeLogEntriesTable(Dp);

                var testMessage = Guid.NewGuid().ToString();

                // action
                SnLog.WriteInformation(testMessage);

                // assert
                var lastEntry = GetLastEventLogEntry();
                var entryData = ParseEventlogEntryData(lastEntry.Message);

                Assert.AreEqual(testMessage, entryData["Message"]);
                Assert.AreEqual("General", entryData["Category"]);
                Assert.AreEqual("Information", entryData["Severity"]);
                Assert.AreEqual(EventLogEntryType.Information, lastEntry.EntryType);
            }
        }
        [TestMethod]
        public void Logging_Audit_ToSql_ViaEntlibConfiguration()
        {
            using (SwindleLogger(new EntLibLoggerAdapter()))
            {
                InitializeLogEntriesTable(Dp);

                var testMessage = Guid.NewGuid().ToString();

                // action
                SnLog.WriteAudit(new TestAuditEvent(testMessage));

                // assert
                var auditEvent = Dp.LoadLastAuditEventsAsync(1).Result.FirstOrDefault();
                Assert.IsNotNull(auditEvent);
                Assert.AreEqual(testMessage, auditEvent.Message);
            }
        }
    }
}
