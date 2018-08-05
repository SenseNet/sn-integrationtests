using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.Diagnostics;

namespace LoggingIntegrationTests
{
    public abstract class LoggerTestBase
    {
        protected void InitializeLogEntriesTable()
        {
            SenseNet.Configuration.ConnectionStrings.ConnectionString = SenseNet.IntegrationTests.Common.ConnectionStrings.ForLoggingTests;
            var proc = DataProvider.CreateDataProcedure("DELETE FROM [LogEntries]");
            proc.CommandType = CommandType.Text;
            proc.ExecuteNonQuery();
            proc = DataProvider.CreateDataProcedure("DBCC CHECKIDENT ('[LogEntries]', RESEED, 1)");
            proc.CommandType = CommandType.Text;
            proc.ExecuteNonQuery();

            //RepositoryVersionInfo.Reset();
        }

        protected EventLogEntry GetLastEventLogEntry()
        {
            var logs = EventLog.GetEventLogs();
            var log = logs.FirstOrDefault(l => l.LogDisplayName == "SenseNet");
            Assert.IsNotNull(log);
            var entries = new List<EventLogEntry>();
            foreach (EventLogEntry entry in log.Entries)
                entries.Add(entry);
            return entries.Last();
        }

        protected Dictionary<string, string> ParseEventlogEntryData(string text)
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

        protected IDisposable SwindleLogger(IEventLogger cheat)
        {
            return new LoggerSwindler(cheat);
        }
        private class LoggerSwindler : IDisposable
        {
            private readonly IEventLogger _backup;
            public LoggerSwindler(IEventLogger cheat)
            {
                _backup = SnLog.Instance;
                SnLog.Instance = cheat;
            }
            public void Dispose()
            {
                SnLog.Instance = _backup;
            }
        }

    }
}
