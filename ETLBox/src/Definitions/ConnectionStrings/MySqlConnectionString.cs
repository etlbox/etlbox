// for string extensions
using MySql.Data.MySqlClient;

namespace ALE.ETLBox
{
    /// <summary>
    /// A helper class for encapsulating a conection string to a MySql server in an object.
    /// Internally the MySqlConnectionStringBuilder is used to access the values of the given connection string.
    /// </summary>
    public class MySqlConnectionString :
        DbConnectionStringWithDbName<MySqlConnectionString, MySqlConnectionStringBuilder>
    {
        public MySqlConnectionString() :
            base()
        { }
        public MySqlConnectionString(string value) :
            base(value)
        { }

        public override string DbName
        {
            get => Builder.Database;
            set => Builder.Database = value;
        }
        public override string MasterDbName => "mysql";

        public static implicit operator MySqlConnectionString(string value) => new MySqlConnectionString(value);
    }
}
