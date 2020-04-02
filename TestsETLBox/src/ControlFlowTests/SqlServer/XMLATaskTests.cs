using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.ControlFlow.SqlServer;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests.SqlServer
{
    [Collection("SSAS ControlFlow")]
    public class XMLATaskTests
    {
        public AdomdConnectionManager DefaultCatalog => new AdomdConnectionManager(Config.SSASConnection.ConnectionString("ControlFlow").CloneWithDbName());
        public XMLATaskTests()
        { }

        internal static string CreateCubeXMLA(string dbName)
        {
            return $@"<Alter AllowCreate=""true"" ObjectExpansion=""ExpandFull"" xmlns=""http://schemas.microsoft.com/analysisservices/2003/engine"">
  <Object>
    <DatabaseID>Cube</DatabaseID>
  </Object>
  <ObjectDefinition>
    <Database xmlns:xsd=""http://www.w3.org/2001/XMLSchema"" xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" xmlns:ddl2=""http://schemas.microsoft.com/analysisservices/2003/engine/2"" xmlns:ddl2_2=""http://schemas.microsoft.com/analysisservices/2003/engine/2/2"" xmlns:ddl100_100=""http://schemas.microsoft.com/analysisservices/2008/engine/100/100"" xmlns:ddl200=""http://schemas.microsoft.com/analysisservices/2010/engine/200"" xmlns:ddl200_200=""http://schemas.microsoft.com/analysisservices/2010/engine/200/200"" xmlns:ddl300=""http://schemas.microsoft.com/analysisservices/2011/engine/300"" xmlns:ddl300_300=""http://schemas.microsoft.com/analysisservices/2011/engine/300/300"" xmlns:ddl400=""http://schemas.microsoft.com/analysisservices/2012/engine/400"" xmlns:ddl400_400=""http://schemas.microsoft.com/analysisservices/2012/engine/400/400"" xmlns:ddl500=""http://schemas.microsoft.com/analysisservices/2013/engine/500"" xmlns:ddl500_500=""http://schemas.microsoft.com/analysisservices/2013/engine/500/500"">
      <ID>{dbName}</ID>
      <Name>{dbName}</Name>
      <Description />
      <DataSourceImpersonationInfo>
        <ImpersonationMode>ImpersonateCurrentUser</ImpersonationMode>
      </DataSourceImpersonationInfo>
    </Database>
  </ObjectDefinition>
</Alter>";
        }

        internal static string DeleteCubeXMLA(string dbName)
        {
            return $@"<Delete xmlns=""http://schemas.microsoft.com/analysisservices/2003/engine"">
  <Object>
    <DatabaseID>{dbName}</DatabaseID>
  </Object>
</Delete>";
        }

        [Fact]
        public void TestCreateAndDelete()
        {
            string dbName = "ETLBox_TestXMLA";
            try
            {
                XmlaTask.ExecuteNonQuery(DefaultCatalog, "Drop cube", DeleteCubeXMLA(dbName));
            }
            catch { }
            XmlaTask.ExecuteNonQuery(DefaultCatalog, "Create cube", CreateCubeXMLA(dbName));
            XmlaTask.ExecuteNonQuery(DefaultCatalog, "Delete cube", DeleteCubeXMLA(dbName));
        }
    }
}
