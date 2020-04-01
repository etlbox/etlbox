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
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class RowTransformationErrorLinkingTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public RowTransformationErrorLinkingTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void ThrowExceptionInFlow()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("RowTransExceptionTest");

            CsvSource<string[]> source = new CsvSource<string[]>("res/RowTransformation/TwoColumns.csv");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>("RowTransExceptionTest", SqlConnection);

            CreateErrorTableTask.DropAndCreate(SqlConnection, "errors");
            DbDestination<ETLBoxError> errorDest = new DbDestination<ETLBoxError>("errors", SqlConnection);

            //Act
            RowTransformation<string[], MySimpleRow> trans = new RowTransformation<string[], MySimpleRow>(
                csvdata =>
                {
                    int no = int.Parse(csvdata[0]);
                    if (no == 2) throw new Exception("Test");
                    return new MySimpleRow()
                    {
                        Col1 = no,
                        Col2 = csvdata[1]
                    };
                });

            source.LinkTo(trans);
            trans.LinkTo(dest);
            trans.LinkErrorTo(errorDest);
            source.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            Assert.Equal(2, RowCountTask.Count(SqlConnection, "RowTransExceptionTest"));
            Assert.Equal(1, RowCountTask.Count(SqlConnection, "errors"));
        }

        [Fact]
        public void ThrowExceptionWithoutHandling()
        {
            //Arrange
            CsvSource<string[]> source = new CsvSource<string[]>("res/RowTransformation/TwoColumns.csv");
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();

            //Act
            RowTransformation<string[], MySimpleRow> trans = new RowTransformation<string[], MySimpleRow>(
                csvdata => throw new InvalidOperationException("Test"));

            source.LinkTo(trans);
            trans.LinkTo(dest);

            //Assert
            Assert.Throws<AggregateException>(() =>
            {
                source.Execute();
                dest.Wait();
            });


        }
    }
}
