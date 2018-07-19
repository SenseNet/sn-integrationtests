# sn-integrationtests
Integration tests for various constellations on the sensenet platform.

-- draft --

The main solution contains a lot of reference projects in the "ReferenceProjects" virtual directory. These projects are in the sensenet ecosystem and can work only if the all affected repositories are cloned in the same local directory (for example d:\dev\github\sensenet).

Do not modify the reference projects. If need to change anything in a reference project, do modification in the original project/solution/repository.

The test projects can use local files of the reference projects (mostly SQL scripts). These files always can be found, if the reference projects are cloned in the same local directory.

Before running the integration tests, the reference projects need to be checked out to the appropriate branch.

The test projects use MS SQL Server databases. The connection strins are gathered in one place so they can be overwritten if needed. The common place of the connection strings: 
```SenseNet.IntegrationTests.Common.ConnectionStrings.cs```
