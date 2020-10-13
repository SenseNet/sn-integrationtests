using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Configuration;
using SenseNet.ContentRepository.Storage;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.Diagnostics;
using SenseNet.Tests;

namespace SenseNet.Packaging.IntegrationTests
{
    //UNDONE: Copy of ExclusiveBlockTestCases.cs from the SenseNet.ContentRepository.Tests
    public abstract class ExclusiveBlockTestCases : TestBase
    {
        private async System.Threading.Tasks.Task Worker(string operationId, ExclusiveBlockType blockType,
            List<string> log, TimeSpan timeout = default)
        {
            var context = new ExclusiveBlockContext
            {
                OperationId = operationId,
                //LockTimeout = TimeSpan.FromSeconds(1),
                //PollingTime = TimeSpan.FromSeconds(0.1),
                LockTimeout = TimeSpan.FromSeconds(60),
                PollingTime = TimeSpan.FromSeconds(1),
            };
            if (timeout != default)
                context.WaitTimeout = timeout;

            log.Add("before block " + operationId);
            Trace.WriteLine("SnTrace: TEST: before block " + operationId);
            await ExclusiveBlock.RunAsync(context, "MyFeature", blockType, CancellationToken.None, async () =>
            {
                //await System.Threading.Tasks.Task.Delay(1500);
                await System.Threading.Tasks.Task.Delay(3000);
                log.Add("in block " + operationId);
                Trace.WriteLine("SnTrace: TEST: in block " + operationId);
            });
            log.Add("after block " + operationId);
            Trace.WriteLine("SnTrace: TEST: after block " + operationId);
        }

        public void TestCase_SkipIfLocked()
        {
            Trace.WriteLine("SnTrace: ----------------------------------------------------------- SkipIfLocked");
            Initialize();
            var log = new List<string>();

            var task1 = Worker("1", ExclusiveBlockType.SkipIfLocked, log);
            var task2 = Worker("2", ExclusiveBlockType.SkipIfLocked, log);
            System.Threading.Tasks.Task.WaitAll(task1, task2);
            Thread.Sleep(100);

            // LOG EXAMPLE:
            // "before block 1"
            // "before block 2"
            // "after block 2"
            // "in block 1"
            // "after block 1"

            var inBlockCount = log.Count(x => x.StartsWith("in block"));
            Assert.AreEqual(1, inBlockCount);
        }

        public void TestCase_WaitForReleased()
        {
            Trace.WriteLine("SnTrace: ----------------------------------------------------------- WaitForReleased");
            Initialize();
            var log = new List<string>();

            var task1 = Worker("1", ExclusiveBlockType.WaitForReleased, log);
            var task2 = Worker("2", ExclusiveBlockType.WaitForReleased, log);
            System.Threading.Tasks.Task.WaitAll(task1, task2);
            Thread.Sleep(100);

            // LOG EXAMPLE:
            // "before block 1"
            // "before block 2"
            // "in block 1"
            // "after block 1"
            // "after block 2"

            var inBlockCount = log.Count(x => x.StartsWith("in block"));
            Assert.AreEqual(1, inBlockCount);
        }

        public void TestCase_WaitAndAcquire()
        {
            Trace.WriteLine("SnTrace: ----------------------------------------------------------- WaitAndAcquire");
            Initialize();
            var log = new List<string>();

            var task1 = Worker("1", ExclusiveBlockType.WaitAndAcquire, log, TimeSpan.FromSeconds(20));
            var task2 = Worker("2", ExclusiveBlockType.WaitAndAcquire, log, TimeSpan.FromSeconds(20));
            System.Threading.Tasks.Task.WaitAll(task1, task2);
            Thread.Sleep(100);

            // LOG EXAMPLE:
            // "before block 1"
            // "before block 2"
            // "in block 1"
            // "after block 1"
            // "in block 2"
            // "after block 2"

            var inBlockCount = log.Count(x => x.StartsWith("in block"));
            Assert.AreEqual(2, inBlockCount);
        }

        private void Initialize()
        {
            Providers.Instance.DataProvider = GetMainDataProvider();
            var ext = GetDataProviderExtension();
            DataStore.DataProvider.SetExtension(typeof(IExclusiveLockDataProviderExtension), ext);

            ext.ReleaseAllAsync(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

            SnTrace.Custom.Enabled = true;
            SnTrace.System.Enabled = true;
        }

        protected abstract DataProvider GetMainDataProvider();
        protected abstract IExclusiveLockDataProviderExtension GetDataProviderExtension();
    }
}
