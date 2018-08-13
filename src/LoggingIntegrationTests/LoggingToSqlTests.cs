using System.Linq;
using LoggingIntegrationTests.Implementations;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.Diagnostics;

namespace LoggingIntegrationTests
{
    [TestClass]
    public class LoggingToSqlTests : LoggerTestBase
    {
        [TestMethod]
        public void Logging_Audit_ToSql_ViaDirectAuditWriter()
        {
            var provider = DataProvider.Current;
            Assert.AreEqual("SqlProvider" ,provider.GetType().Name);
            InitializeLogEntriesTable();

            SnLog.AuditEventWriter = new DatabaseAuditEventWriter();

            var testMessage = "Msg1";

            // action
            SnLog.WriteAudit(new TestAuditEvent(testMessage));

            // assert
            var auditEvent = DataProvider.Current.LoadLastAuditLogEntries(1).FirstOrDefault();
            Assert.IsNotNull(auditEvent);
            Assert.AreEqual(testMessage, auditEvent.Message);
        }
    }
}
