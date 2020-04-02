using Npgsql;

namespace ALE.ETLBox
{
    /// <summary>
    /// A helper class for encapsulating a conection string to a MySql server in an object.
    /// Internally the MySqlConnectionStringBuilder is used to access the values of the given connection string.
    /// </summary>
    public class PostgresConnectionString :
        DbConnectionStringWithDbName<PostgresConnectionString, NpgsqlConnectionStringBuilder>
    {
        public PostgresConnectionString() :
            base()
        { }
        public PostgresConnectionString(string value) :
            base(value)
        { }

        public override string DbName
        {
            get => Builder.Database;
            set => Builder.Database = value;
        }
        public override string MasterDbName => "postgres";

        public static implicit operator PostgresConnectionString(string value) => new PostgresConnectionString(value);
    }
}
