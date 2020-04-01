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
    [Collection("DataFlow Source and Destination")]
    public class DbMergeAcrossDatabasesTests
    {
        public static IEnumerable<object[]> MixedSourceDestinations() => new[] {
            //Same DB
            new object[] {
                (IConnectionManager)Config.SqlConnection.ConnectionManager("DataFlowSource"),
                (IConnectionManager)Config.SqlConnection.ConnectionManager("DataFlowDestination")
            },
            new object[] {
                (IConnectionManager)Config.SqlConnection.ConnectionManager("DataFlowSource"),
                (IConnectionManager)Config.SQLiteConnection.ConnectionManager("DataFlowDestination")
            },
            new object[] {
                (IConnectionManager)Config.SqlConnection.ConnectionManager("DataFlowSource"),
                (IConnectionManager)Config.MySqlConnection.ConnectionManager("DataFlowDestination")
            },
            new object[] {
                (IConnectionManager)Config.SqlConnection.ConnectionManager("DataFlowSource"),
                (IConnectionManager)Config.PostgresConnection.ConnectionManager("DataFlowDestination")
            }
        };

        public DbMergeAcrossDatabasesTests(DatabaseSourceDestinationFixture dbFixture)
        {
        }

        public class Name
        {
            public string ID { get; set; }

            public string FIRST_NAME { get; set; }

            public string LAST_NAME { get; set; }
        }

        public class People : MergeableRow
        {
            [CompareColumn]
            public string FirstName { get; set; }

            [CompareColumn]
            public string LastName { get; set; }

            [IdColumn]
            public string Id { get; set; }
        }


        [Theory, MemberData(nameof(MixedSourceDestinations))]
        public void Test(IConnectionManager sourceConnection, IConnectionManager destConnection)
        {
            //Arrange
            string QB = ConnectionManagerSpecifics.GetBeginQuotation(destConnection);
            string QE = ConnectionManagerSpecifics.GetEndQuotation(destConnection);
            CreateSourceAndDestinationTables(sourceConnection, destConnection, QB, QE);

            //Act
            var nameSource = new DbSource<Name>("Name", sourceConnection);
            var personMerge = new DbMerge<People>("People", destConnection);

            var transform = new RowTransformation<Name, People>(d =>
            {
                return new People()
                {
                    FirstName = d.FIRST_NAME,
                    LastName = d.LAST_NAME,
                    Id = d.ID
                };
            });

            nameSource.LinkTo(transform);
            transform.LinkTo(personMerge);

            nameSource.Execute();
            personMerge.Wait();

            //Assert
            Assert.Equal(1, RowCountTask.Count(destConnection, "People", $"{QB}Id{QE} = 1 AND {QB}FirstName{QE} = 'Bugs' AND {QB}LastName{QE} IS NULL"));
            Assert.Equal(1, RowCountTask.Count(destConnection, "People", $"{QB}Id{QE} = 2 AND {QB}FirstName{QE} IS NULL AND {QB}LastName{QE} = 'Pig'"));
            Assert.Equal(1, RowCountTask.Count(destConnection, "People", $"{QB}Id{QE} = 3 AND {QB}FirstName{QE} = 'Franky' AND {QB}LastName{QE} IS NULL"));

        }

        private static void CreateSourceAndDestinationTables(IConnectionManager sourceConnection, IConnectionManager destConnection, string QB, string QE)
        {
            DropTableTask.DropIfExists(sourceConnection, "Name");
            CreateTableTask.Create(sourceConnection, "Name",
                                new List<TableColumn>() {
                    new TableColumn("ID", "INT", false, true, true),
                    new TableColumn("FIRST_NAME", "NVARCHAR(100)", true),
                    new TableColumn("LAST_NAME", "NVARCHAR(100)", true)
                                });
            DropTableTask.DropIfExists(sourceConnection, "People");
            CreateTableTask.Create(destConnection, "People",
                new List<TableColumn>() {
                    new TableColumn("Id", "INT", false, true),
                    new TableColumn("FirstName", "NVARCHAR(100)", true),
                    new TableColumn("LastName", "NVARCHAR(100)", true)
                });

            SqlTask.ExecuteNonQuery(sourceConnection, "Test data",
                "INSERT INTO Name (FIRST_NAME, LAST_NAME) VALUES ('Bugs', NULL)");
            SqlTask.ExecuteNonQuery(sourceConnection, "Test data",
            "INSERT INTO Name (FIRST_NAME, LAST_NAME) VALUES (NULL, 'Pig')");
            SqlTask.ExecuteNonQuery(sourceConnection, "Test data",
                "INSERT INTO Name (FIRST_NAME, LAST_NAME) VALUES ('Franky', NULL)");

            SqlTask.ExecuteNonQuery(destConnection, "Test data",
                $"INSERT INTO {QB}People{QE} ({QB}Id{QE}, {QB}FirstName{QE}, {QB}LastName{QE}) VALUES (1, 'Bugs', NULL)");
            SqlTask.ExecuteNonQuery(destConnection, "Test data",
                $"INSERT INTO {QB}People{QE} ({QB}Id{QE}, {QB}FirstName{QE}, {QB}LastName{QE}) VALUES (2, 'Peggy', NULL)");
        }

    }
}
