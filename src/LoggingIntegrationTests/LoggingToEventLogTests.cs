using System;
using System.Diagnostics;
using LoggingIntegrationTests.Implementations;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Diagnostics;

namespace LoggingIntegrationTests
{
    [TestClass]
    public class LoggingToEventLogTests : LoggerTestBase
    {
        private static readonly string LogName = "SenseNet";
        private static readonly string LogSource = "SenseNetInstrumentation";

        [TestMethod]
        public void Logging_Audit_ToEventLog()
        {
            using (SwindleLogger(new SnEventLogger(LogName, LogSource)))
            {
                var testValue = Guid.NewGuid().ToString();

                // action
                SnLog.WriteAudit(new TestAuditEvent(testValue));

                // assert
                var lastEntry = GetLastEventLogEntry();
                var entryData = ParseEventlogEntryData(lastEntry.Message);

                Assert.AreEqual(testValue, entryData["Message"]);
                Assert.AreEqual("Audit", entryData["Category"]);
                Assert.AreEqual("Verbose", entryData["Severity"]);
                Assert.AreEqual(EventLogEntryType.Information, lastEntry.EntryType);
            }
        }

        [TestMethod]
        public void Logging_Information_ToEventLog()
        {
            using (SwindleLogger(new SnEventLogger(LogName, LogSource)))
            {
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
    }
}
