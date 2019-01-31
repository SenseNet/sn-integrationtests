using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Diagnostics;

namespace SenseNet.ContentRepository.IntegrationTests
{
    public class Initializer
    {
        [AssemblyInitialize]
        public static void StartAllTests(TestContext testContext)
        {
            SnTrace.SnTracers.Clear();
            SnTrace.SnTracers.Add(new SnFileSystemTracer());
            SnTrace.SnTracers.Add(new SnDebugViewTracer());
            SnTrace.Test.Enabled = true;
            SnTrace.Test.Write("------------------------------------------------------");
        }
    }
}
