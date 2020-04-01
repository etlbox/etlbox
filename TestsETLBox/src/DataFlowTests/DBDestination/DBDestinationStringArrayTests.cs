using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DbDestinationStringArrayTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public DbDestinationStringArrayTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Theory, MemberData(nameof(Connections))]
        public void WithSqlNotMatchingColumns(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "SourceNotMatchingCols");
            s2c.InsertTestData();
            SqlTask.ExecuteNonQuery(connection, "Create destination table",
                $@"CREATE TABLE destination_notmatchingcols
                ( col3 VARCHAR(100) NULL
                , col4 VARCHAR(100) NULL
                , {s2c.QB}Col1{s2c.QE} VARCHAR(100) NULL)");

            //Act
            DbSource<string[]> source = new DbSource<string[]>(
                $"SELECT {s2c.QB}Col1{s2c.QE}, {s2c.QB}Col2{s2c.QE} FROM {s2c.QB}SourceNotMatchingCols{s2c.QE}",
                connection
                );
            DbDestination<string[]> dest = new DbDestination<string[]>("destination_notmatchingcols", connection);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(connection, "destination_notmatchingcols"));
            Assert.Equal(1, RowCountTask.Count(connection, "destination_notmatchingcols", $"col3 = '1' AND col4='Test1'"));
            Assert.Equal(1, RowCountTask.Count(connection, "destination_notmatchingcols", $"col3 = '2' AND col4='Test2'"));
            Assert.Equal(1, RowCountTask.Count(connection, "destination_notmatchingcols", $"col3 = '3' AND col4='Test3'"));
        }


        [Theory, MemberData(nameof(Connections))]
        public void WithLessColumnsInDestination(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "SourceTwoColumns");
            s2c.InsertTestData();
            SqlTask.ExecuteNonQuery(connection, "Create destination table",
                @"CREATE TABLE destination_onecolumn
                (colx varchar (100) not null )");

            //Act
            DbSource<string[]> source = new DbSource<string[]>("SourceTwoColumns", connection);
            DbDestination<string[]> dest = new DbDestination<string[]>("destination_onecolumn", connection);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(connection, "destination_onecolumn"));
            Assert.Equal(1, RowCountTask.Count(connection, "destination_onecolumn", "colx = '1'"));
            Assert.Equal(1, RowCountTask.Count(connection, "destination_onecolumn", "colx = '2'"));
            Assert.Equal(1, RowCountTask.Count(connection, "destination_onecolumn", "colx = '3'"));
        }

        [Theory, MemberData(nameof(Connections))]
        public void WithAdditionalNullableCol(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "source_additionalnullcol");
            s2c.InsertTestData();
            SqlTask.ExecuteNonQuery(connection, "Create destination table", @"CREATE TABLE destination_additionalnullcol
                (col1 VARCHAR(100) NULL, col2 VARCHAR(100) NULL, col3 VARCHAR(100) NULL)");

            //Act
            DbSource<string[]> source = new DbSource<string[]>("source_additionalnullcol", connection);
            DbDestination<string[]> dest = new DbDestination<string[]>("destination_additionalnullcol", connection);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            s2c.AssertTestData();
        }

        [Theory, MemberData(nameof(Connections))]
        public void WithAdditionalNotNullCol(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "source_additionalnotnullcol");
            s2c.InsertTestData();
            SqlTask.ExecuteNonQuery(connection, "Create destination table", @"CREATE TABLE destination_additionalnotnullcol
                (col1 VARCHAR(100) NULL, col2 VARCHAR(100) NULL, col3 VARCHAR(100) NOT NULL)");

            //Act
            DbSource<string[]> source = new DbSource<string[]>("source_additionalnotnullcol", connection);
            DbDestination<string[]> dest = new DbDestination<string[]>("destination_additionalnotnullcol", connection);
            source.LinkTo(dest);
            Assert.Throws<AggregateException>(() =>
            {
                source.Execute();
                dest.Wait();
            });
        }
    }
}
