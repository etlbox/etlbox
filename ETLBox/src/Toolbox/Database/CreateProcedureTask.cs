using ALE.ETLBox.ConnectionManager;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Creates or updates a procedure.
    /// </summary>
    /// <example>
    /// <code>
    /// CRUDProcedureTask.CreateOrAlter("demo.proc1", "select 1 as test");
    /// </code>
    /// </example>
    public class CreateProcedureTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"{CreateOrAlterSql} procedure {ProcedureName}";
        public void Execute()
        {
            if (!DbConnectionManager.SupportProcedures)
                throw new ETLBoxNotSupportedException("This task is not supported!");

            IsExisting = new IfProcedureExistsTask(ProcedureName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            
            if (IsExisting && !DbConnectionManager.SupportProcedureCreateOrReplace && !DbConnectionManager.SupportProcedureAlter)
                new DropProcedureTask(ProcedureName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Drop();
            
            new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public string ProcedureName { get; set; }
        public ObjectNameDescriptor PN => new ObjectNameDescriptor(ProcedureName, QB, QE);
        public string ProcedureDefinition { get; set; }
        public IList<ProcedureParameter> ProcedureParameters { get; set; }
        public string Sql => $@"{CreateOrAlterSql} PROCEDURE {PN.QuotatedFullName}{ParameterDefinition}{Language}
{AS}
{BEGIN}

{ProcedureDefinition}

{END}
        ";

        public CreateProcedureTask()
        {

        }
        public CreateProcedureTask(string procedureName, string procedureDefinition) : this()
        {
            this.ProcedureName = procedureName;
            this.ProcedureDefinition = procedureDefinition;
        }

        public CreateProcedureTask(string procedureName, string procedureDefinition, IList<ProcedureParameter> procedureParameter) : this(procedureName, procedureDefinition)
        {
            this.ProcedureParameters = procedureParameter;
        }

        public CreateProcedureTask(ProcedureDefinition definition) : this()
        {
            this.ProcedureName = definition.Name;
            this.ProcedureDefinition = definition.Definition;
            this.ProcedureParameters = definition.Parameter;
        }

        public static void CreateOrAlter(string procedureName, string procedureDefinition) => new CreateProcedureTask(procedureName, procedureDefinition).Execute();
        public static void CreateOrAlter(string procedureName, string procedureDefinition, IList<ProcedureParameter> procedureParameter)
            => new CreateProcedureTask(procedureName, procedureDefinition, procedureParameter).Execute();
        public static void CreateOrAlter(ProcedureDefinition procedure)
            => new CreateProcedureTask(procedure).Execute();
        public static void CreateOrAlter(IConnectionManager connectionManager, string procedureName, string procedureDefinition)
            => new CreateProcedureTask(procedureName, procedureDefinition) { ConnectionManager = connectionManager }.Execute();
        public static void CreateOrAlter(IConnectionManager connectionManager, string procedureName, string procedureDefinition, IList<ProcedureParameter> procedureParameter)
            => new CreateProcedureTask(procedureName, procedureDefinition, procedureParameter) { ConnectionManager = connectionManager }.Execute();
        public static void CreateOrAlter(IConnectionManager connectionManager, ProcedureDefinition procedure)
            => new CreateProcedureTask(procedure) { ConnectionManager = connectionManager }.Execute();

        bool IsExisting { get; set; }
        string CreateOrAlterSql
        {
            get
            {
                if (DbConnectionManager.SupportProcedureCreateOrReplace)
                    return "CREATE OR REPLACE";

                if (DbConnectionManager.SupportProcedureAlter)
                    return IsExisting ? "ALTER" : "CREATE";
                
                return "CREATE";
            }
        }
        string ParameterDefinition
        {
            get
            {
                string result = "";
                
                if (ConnectionType == ConnectionManagerType.Postgres || ConnectionType == ConnectionManagerType.MySql)
                    result += "(";
                
                result += ProcedureParameters?.Count > 0 
                    ? String.Join(",", ProcedureParameters.Select(par => ParameterSql(par)))
                    : String.Empty;
                
                if (ConnectionType == ConnectionManagerType.Postgres || ConnectionType == ConnectionManagerType.MySql)
                    result += ")";
                
                return result;
            }
        }

        public string ParameterSql(ProcedureParameter par)
        {
            string sql = Environment.NewLine + "";
            
            if (ConnectionType == ConnectionManagerType.SqlServer)
                sql += "@";
            
            if (ConnectionType == ConnectionManagerType.MySql)
                sql += par.Out ? "OUT " : "IN ";
            
            sql += $@"{par.Name} {par.DataType}";
            
            if (par.HasDefaultValue && ConnectionType != ConnectionManagerType.MySql)
                sql += $" = {par.DefaultValue}";
            
            if (par.Out && ConnectionType != ConnectionManagerType.MySql)
                sql += " OUT";
            
            if (par.ReadOnly)
                sql += " READONLY";
            
            return sql;
        }

        string Language => this.ConnectionType == ConnectionManagerType.Postgres 
            ? Environment.NewLine + "LANGUAGE SQL"
            : "";
        string BEGIN => this.ConnectionType == ConnectionManagerType.Postgres ? "$$" : "BEGIN";
        string END => this.ConnectionType == ConnectionManagerType.Postgres ? "$$" : "END";
        string AS => this.ConnectionType == ConnectionManagerType.MySql ? "" : "AS";
    }
}
