using System;
using System.Diagnostics;
using System.Linq;
using LoggingIntegrationTests.Implementations;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.Diagnostics;

namespace LoggingIntegrationTests
{
    [TestClass]
    public class EntlibLoggerTests : LoggerTestBase
    {
        [TestMethod]
        public void Logging_Information_ToEntlib()
        {
            using (SwindleLogger(new EntLibLoggerAdapter()))
            {
                var provider = DataProvider.Current;
                Assert.AreEqual("SqlProvider", provider.GetType().Name);
                InitializeLogEntriesTable();

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
                var provider = DataProvider.Current;
                Assert.AreEqual("SqlProvider", provider.GetType().Name);
                InitializeLogEntriesTable();

                var testMessage = Guid.NewGuid().ToString();

                // action
                SnLog.WriteAudit(new TestAuditEvent(testMessage));

                // assert
                var auditEvent = DataProvider.Current.LoadLastAuditLogEntries(1).FirstOrDefault();
                Assert.IsNotNull(auditEvent);
                Assert.AreEqual(testMessage, auditEvent.Message);
            }
        }
    }
}
