namespace SenseNet.IntegrationTests.Common
{
    public static class ConnectionStrings
    {
        public static readonly string ForBlobStorageTests =
            @"Data Source=.\SQL2016;Integrated Security=SSPI;Persist Security Info=False";

        public static readonly string ForPackagingTests =
            @"Initial Catalog=sn7tests;Data Source=.\SQL2016;Integrated Security=SSPI;Persist Security Info=False";

        public static readonly string ForCentralizedIndexingTests =
            @"Initial Catalog=sn7tests;Data Source=.\SQL2016;Integrated Security=SSPI;Persist Security Info=False";

        public static readonly string ForLoggingTests =
            @"Initial Catalog=sn7tests;Data Source=.\SQL2016;Integrated Security=SSPI;Persist Security Info=False";

        public static readonly string ForContentRepositoryTests =
            @"Initial Catalog=sn7tests;Data Source=.\SQL2016;Integrated Security=SSPI;Persist Security Info=False";

        public static readonly string ForStorageTests =
            @"Initial Catalog=sn71tests;Data Source=.\SQL2016;Integrated Security=SSPI;Persist Security Info=False";
    }
}
