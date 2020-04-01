using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using System.Dynamic;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DbDestinationDynamicObjectTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");
        public static IEnumerable<object[]> ConnectionsNoSQLite => Config.AllConnectionsWithoutSQLite("DataFlow");

        public DbDestinationDynamicObjectTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Theory, MemberData(nameof(Connections))]
        public void SourceMoreColumnsThanDestination(IConnectionManager connection)
        {
            //Arrange
            FourColumnsTableFixture source4Columns = new FourColumnsTableFixture(connection, "SourceDynamic4Cols");
            source4Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(connection, "DestinationDynamic2Cols");

            //Act
            DbSource<ExpandoObject> source = new DbSource<ExpandoObject>("SourceDynamic4Cols", connection);
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>("DestinationDynamic2Cols", connection);

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Theory, MemberData(nameof(Connections))]
        public void DestinationMoreColumnsThanSource(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(connection, "SourceDynamicDiffCols");
            source2Columns.InsertTestData();
            CreateTableTask.Create(connection, "DestinationDynamicDiffCols",
                new List<TableColumn>() {
                    new TableColumn("Col5", "VARCHAR(100)", allowNulls:true),
                    new TableColumn("Col2", "VARCHAR(100)",allowNulls:true),
                    new TableColumn("Col1", "INT",allowNulls:true),
                    new TableColumn("ColX", "INT",allowNulls:true),
                });

            //Act
            DbSource<ExpandoObject> source = new DbSource<ExpandoObject>("SourceDynamicDiffCols", connection);
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>("DestinationDynamicDiffCols", connection);

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            string QB = ConnectionManagerSpecifics.GetBeginQuotation(connection);
            string QE = ConnectionManagerSpecifics.GetEndQuotation(connection);
            Assert.Equal(3, RowCountTask.Count(connection, "DestinationDynamicDiffCols"));
            Assert.Equal(1, RowCountTask.Count(connection, "DestinationDynamicDiffCols", $"{QB}Col1{QE} = 1 AND {QB}Col2{QE}='Test1' AND {QB}Col5{QE} IS NULL AND {QB}ColX{QE} IS NULL"));
            Assert.Equal(1, RowCountTask.Count(connection, "DestinationDynamicDiffCols", $"{QB}Col1{QE} = 2 AND {QB}Col2{QE}='Test2' AND {QB}Col5{QE} IS NULL AND {QB}ColX{QE} IS NULL"));
            Assert.Equal(1, RowCountTask.Count(connection, "DestinationDynamicDiffCols", $"{QB}Col1{QE} = 3 AND {QB}Col2{QE}='Test3' AND {QB}Col5{QE} IS NULL AND {QB}ColX{QE} IS NULL"));
        }

        [Theory, MemberData(nameof(ConnectionsNoSQLite))]
        public void DestinationWithIdentityColumn(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(connection, "SourceDynamicIDCol");
            source2Columns.InsertTestData();
            CreateTableTask.Create(connection, "DestinationDynamicIdCol",
                new List<TableColumn>() {
                    new TableColumn("Id", "BIGINT", allowNulls:false, isPrimaryKey:true, isIdentity:true),
                    new TableColumn("Col2", "VARCHAR(100)",allowNulls:true),
                    new TableColumn("Col1", "INT",allowNulls:true),
                    new TableColumn("ColX", "INT",allowNulls:true),
                });

            //Act
            DbSource<ExpandoObject> source = new DbSource<ExpandoObject>("SourceDynamicIDCol", connection);
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>("DestinationDynamicIdCol", connection);

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            string QB = ConnectionManagerSpecifics.GetBeginQuotation(connection);
            string QE = ConnectionManagerSpecifics.GetEndQuotation(connection);
            Assert.Equal(3, RowCountTask.Count(connection, "DestinationDynamicIdCol"));
            Assert.Equal(1, RowCountTask.Count(connection, "DestinationDynamicIdCol", $"{QB}Col1{QE} = 1 AND {QB}Col2{QE}='Test1' AND {QB}Id{QE} > 0 AND {QB}ColX{QE} IS NULL"));
            Assert.Equal(1, RowCountTask.Count(connection, "DestinationDynamicIdCol", $"{QB}Col1{QE} = 2 AND {QB}Col2{QE}='Test2' AND {QB}Id{QE} > 0 AND {QB}ColX{QE} IS NULL"));
            Assert.Equal(1, RowCountTask.Count(connection, "DestinationDynamicIdCol", $"{QB}Col1{QE} = 3 AND {QB}Col2{QE}='Test3' AND {QB}Id{QE} > 0 AND {QB}ColX{QE} IS NULL"));
        }
    }
}
