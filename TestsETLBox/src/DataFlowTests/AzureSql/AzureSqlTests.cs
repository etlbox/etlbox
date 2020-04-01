using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.ControlFlow.SqlServer;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class AzureSqlTests
    {
        public static SqlConnectionManager AzureSqlConnection => Config.AzureSqlConnection.ConnectionManager("DataFlow");

        public static SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");


        public AzureSqlTests(DataFlowDatabaseFixture dbFixture)
        {
            CleanUpSchemaTask.CleanUp(AzureSqlConnection, "[source]");
            CleanUpSchemaTask.CleanUp(AzureSqlConnection, "[dest]");
            CreateSchemaTask.Create(AzureSqlConnection, "[source]");
            CreateSchemaTask.Create(AzureSqlConnection, "[dest]");
        }

        public class MySimpleRow : MergeableRow
        {
            [IdColumn]
            public int Col1 { get; set; }
            [CompareColumn]
            public string Col2 { get; set; }
        }

        [Fact]
        public void ReadAndWriteToAzure()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(AzureSqlConnection, "[source].[AzureSource]");
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(AzureSqlConnection, "[dest].[AzureDestination]");

            //Act
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>("[source].[AzureSource]", AzureSqlConnection);
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>("[dest].[AzureDestination]", AzureSqlConnection);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void MergeIntoAzure()
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(SqlConnection, "DBMergeSource");
            s2c.InsertTestData();
            s2c.InsertTestDataSet2();
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(AzureSqlConnection, "[dest].[AzureMergeDestination]");
            d2c.InsertTestDataSet3();
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>("DBMergeSource", SqlConnection);

            //Act
            DbMerge<MySimpleRow> dest = new DbMerge<MySimpleRow>("[dest].[AzureMergeDestination]", AzureSqlConnection);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(6, RowCountTask.Count(AzureSqlConnection, "[dest].[AzureMergeDestination]", $"{d2c.QB}Col1{d2c.QE} BETWEEN 1 AND 7 AND {d2c.QB}Col2{d2c.QE} LIKE 'Test%'"));
            Assert.True(dest.DeltaTable.Count == 7);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "U").Count() == 2);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "D" && row.Col1 == 10).Count() == 1);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "I").Count() == 3);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "E" && row.Col1 == 1).Count() == 1);

        }


    }
}
