using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.ContentRepository;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.SqlClient;
using SenseNet.Diagnostics;
using SenseNet.Packaging;
using SenseNet.Tests;

namespace LoggingIntegrationTests
{
    internal class TestAuditEvent : IAuditEvent
    {
        public int EventId { get; }
        public string Message { get; }
        public string Title { get; }

        public TestAuditEvent(string message, string title = null, int eventId = 0)
        {
            Message = message;
            Title = title ?? message;
            EventId = eventId;
        }
    }

    [TestClass]
    public class LoggingToSqlTests
    {
        [TestMethod]
        public void Logging_Audit_ToSql()
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


        /* ============================================================================ */

        private void InitializeLogEntriesTable()
        {
            // preparing database
            SenseNet.Configuration.ConnectionStrings.ConnectionString = SenseNet.IntegrationTests.Common.ConnectionStrings.ForLoggingTests;
            var proc = DataProvider.CreateDataProcedure("DELETE FROM [LogEntries]");
            proc.CommandType = CommandType.Text;
            proc.ExecuteNonQuery();
            proc = DataProvider.CreateDataProcedure("DBCC CHECKIDENT ('[LogEntries]', RESEED, 1)");
            proc.CommandType = CommandType.Text;
            proc.ExecuteNonQuery();

            RepositoryVersionInfo.Reset();
        }
    }
}
