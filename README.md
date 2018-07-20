# sensenet integration tests
This repository contains integration tests for various constellations on the sensenet platform. It is intended to be used for testing scenarios when a simple unit test is not sufficient and we need to test a certain feature that requires multiple layers or components to be present - for example a real database.

## Test solution
The main solution contains reference projects in the "ReferenceProjects" virtual directory. The source of these projects reside in **multiple repositories** in the sensenet ecosystem. This means tests will work only if **all referenced repositories are cloned** in the same local directory (for example `d:\dev\github\sensenet`) and the appropriate **branch** is checked out in those repositories.

> **Important**: please do not modify referenced projects here. If you need to change anything in a reference project, make the modification in the original project/solution/repository.

Test projects can use local files of reference projects (mostly SQL scripts). These files are available if the reference projects are cloned correctly in the same local directory.

## Running tests
It is possible to simply get all the necessary repositories, compile the solution and run tests.

The test projects use MS SQL Server databases. The connection strings are gathered in one place so they can be overwritten if needed. The common place of the connection strings:

```txt
SenseNet.IntegrationTests.Common.ConnectionStrings.cs
```
