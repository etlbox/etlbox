namespace ALE.ETLBox
{
    public interface IDbConnectionStringWithDbName :
        IDbConnectionString
    {
        bool HasDbName { get; }
        string DbName { get; set; }

        bool HasMasterDbName { get; }
        string MasterDbName { get; }

        IDbConnectionStringWithDbName CloneWithDbName(string value = null);
        IDbConnectionStringWithDbName CloneWithMasterDbName();
    }
}
