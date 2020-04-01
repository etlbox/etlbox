using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using CsvHelper.Configuration.Attributes;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Serialization;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class XmlSourceTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public XmlSourceTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void XmlOnlyElements()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("XmlSource2Cols");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>("XmlSource2Cols", Connection);

            //Act
            XmlSource<MySimpleRow> source = new XmlSource<MySimpleRow>("res/XmlSource/TwoColumnsOnlyElements.xml", ResourceType.File);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
        
        [XmlRoot("MySimpleRow")]
        public class MyAttributeRow
        {
            [XmlAttribute]
            public int Col1 { get; set; }
            [XmlAttribute]
            public string Col2 { get; set; }
        }

        [Fact]
        public void XmlOnlyAttributes()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("XmlSource2ColsAttribute");
            DbDestination<MyAttributeRow> dest = new DbDestination<MyAttributeRow>("XmlSource2ColsAttribute", Connection);

            //Actt
            XmlSource<MyAttributeRow> source = new XmlSource<MyAttributeRow>("res/XmlSource/TwoColumnsOnlyAttributes.xml", ResourceType.File);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

    }
}
