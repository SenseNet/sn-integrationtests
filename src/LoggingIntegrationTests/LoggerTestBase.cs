using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Security;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Common.Storage.Data;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.Diagnostics;

namespace LoggingIntegrationTests
{
    public abstract class LoggerTestBase
    {
        protected void InitializeLogEntriesTable(IDbCommandFactory dataProvider)
        {
            using (var ctx = new SnDataContext(dataProvider))
            {
                ctx.ExecuteNonQueryAsync("DELETE FROM [LogEntries]").Wait();
                ctx.ExecuteNonQueryAsync("DBCC CHECKIDENT ('[LogEntries]', RESEED, 1)").Wait();
            }
        }

        protected EventLogEntry GetLastEventLogEntry()
        {
            var logs = EventLog.GetEventLogs();
            var log = logs.FirstOrDefault(l =>
            {
                try
                {
                    // Accessing to any log-property can cause SecurityException if the
                    // matching registry key cannot be accessed for the app user
                    return l.LogDisplayName == "SenseNet";
                }
                catch (SecurityException)
                {
                    return false;
                }
            });
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
