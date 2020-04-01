using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CsvSourceIdentityColumTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CsvSourceIdentityColumTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void IdentityAtPosition1()
        {
            //Arrange
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture("CsvDestination4Columns", identityColumnIndex: 0);
            DbDestination<string[]> dest = new DbDestination<string[]>("CsvDestination4Columns", Connection);

            //Act
            CsvSource<string[]> source = new CsvSource<string[]>("res/CsvSource/ThreeColumnsNoId.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
        }

        [Fact]
        public void IdentityInTheMiddle()
        {
            //Arrange
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture("CsvDestination4Columns", identityColumnIndex: 2);
            DbDestination<string[]> dest = new DbDestination<string[]>("CsvDestination4Columns", Connection);

            //Act
            CsvSource<string[]> source = new CsvSource<string[]>("res/CsvSource/ThreeColumnsNoId.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
        }


        [Fact]
        public void IdentityAtTheEnd()
        {
            //Arrange
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture("CsvDestination4Columns", identityColumnIndex: 3);
            DbDestination<string[]> dest = new DbDestination<string[]>("CsvDestination4Columns", Connection);

            //Act
            CsvSource<string[]> source = new CsvSource<string[]>("res/CsvSource/ThreeColumnsNoId.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
        }
    }
}
