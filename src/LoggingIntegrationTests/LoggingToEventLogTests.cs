using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Diagnostics;

namespace LoggingIntegrationTests
{
    [TestClass]
    public class LoggingToEventLogTests
    {
        [TestMethod]
        public void Logging_Audit_ToEventLog()
        {
            using (new SnEventLoggerSwindler())
            {
                var testValue = Guid.NewGuid().ToString();

                // action
                SnLog.WriteAudit(new TestAuditEvent(testValue));

                // assert
                var lastEntry = GetLastEntry();
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
            using (new SnEventLoggerSwindler())
            {
                var testMessage = Guid.NewGuid().ToString();

                // action
                SnLog.WriteInformation(testMessage);

                // assert
                var lastEntry = GetLastEntry();
                var entryData = ParseEventlogEntryData(lastEntry.Message);

                Assert.AreEqual(testMessage, entryData["Message"]);
                Assert.AreEqual("General", entryData["Category"]);
                Assert.AreEqual("Information", entryData["Severity"]);
                Assert.AreEqual(EventLogEntryType.Information, lastEntry.EntryType);
            }
        }

        /* ============================================================= */

        private class SnEventLoggerSwindler : IDisposable
        {
            private readonly IEventLogger _backup;
            public SnEventLoggerSwindler()
            {
                _backup = SnLog.Instance;
                SnLog.Instance = new SnEventLogger("SenseNet", "SenseNetInstrumentation");
            }
            public void Dispose()
            {
                SnLog.Instance = _backup;
            }
        }

        private EventLogEntry GetLastEntry()
        {
            var logs = EventLog.GetEventLogs();
            var log = logs.FirstOrDefault(l => l.LogDisplayName == "SenseNet");
            Assert.IsNotNull(log);
            var entries = new List<EventLogEntry>();
            foreach (EventLogEntry entry in log.Entries)
                entries.Add(entry);
            return entries.Last();
        }

        private Dictionary<string, string> ParseEventlogEntryData(string text)
        {
            var result = new Dictionary<string, string>();
            var fields = text.Split(new[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);
            var index = 0;
            while (true)
            {
                var field = fields[index++];
                var p = field.IndexOf(':');
                var name = field.Substring(0, p);
                var value = field.Length > p ? field.Substring(p + 1).Trim() : string.Empty;
                if (name != "Extended Properties")
                {
                    result.Add(name, value);
                    continue;
                }
                var extendedValue = new StringBuilder(value);
                for (int i = index; i < fields.Length; i++)
                    extendedValue.Append(", ").Append(fields[i]);
                result.Add(name, extendedValue.ToString());
                break;
            }
            return result;
        }
    }
}
