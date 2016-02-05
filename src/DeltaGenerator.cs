//todo: DeltaHandle soll beliebiger Datentyp sein
//todo: key soll auch deltaHandle sein können
//todo: test mit adventureworks

using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using Microsoft.SqlServer.Management.Smo;
using RazorEngine;

namespace SqlDeltaGenerator
{
    public class DeltaGenerator : INotifyPropertyChanged
    {


        public const string Key = "Key";
        public const string Attribute = "Attribute";
        public const string DeltaHandle = "DeltaHandle";
        public const string Ignore = "Ignore";

        public const string BufferPrefix = "BUFFER_";
        public const string StagingPrefix = "STAGING_";

        public string ConnectionString
        {
            get
            {
                return string.Format("Server={0};Database={1};Trusted_Connection=true;", SourceServer, SourceDatabase);
            }
        }

        public string SourceServer { get; set; }
        public string SourceSchema { get; set; }
        public string SourceTable { get; set; }
        public string SourceDatabase { get; set; }


        public string DestinationServer { get; set; }
        public string DestinationTable { get; set; }
        public string DestinationDatabase { get; set; }

        public bool Historizing { get; set; }

        public DeltaGenerator()
        {
            SourceServer = "localhost";
            SourceSchema = "Sales";
            SourceDatabase = "AdventureWorks2012";
            SourceTable = "SalesOrderHeader";
            DestinationDatabase = "StagingDb";
            DestinationServer = "localhost";

            SourceServer = !string.IsNullOrWhiteSpace(Properties.Settings.Default["SourceServer"].ToString()) ? Properties.Settings.Default["SourceServer"].ToString() : SourceServer;
            SourceSchema = !string.IsNullOrWhiteSpace(Properties.Settings.Default["SourceSchema"].ToString()) ? Properties.Settings.Default["SourceSchema"].ToString() : SourceSchema;
            SourceDatabase = !string.IsNullOrWhiteSpace(Properties.Settings.Default["SourceDatabase"].ToString()) ? Properties.Settings.Default["SourceDatabase"].ToString() : SourceDatabase;
            SourceTable = !string.IsNullOrWhiteSpace(Properties.Settings.Default["SourceTable"].ToString()) ? Properties.Settings.Default["SourceTable"].ToString() : SourceTable;
            DestinationDatabase = !string.IsNullOrWhiteSpace(Properties.Settings.Default["DestinationDatabase"].ToString()) ? Properties.Settings.Default["DestinationDatabase"].ToString() : DestinationDatabase;
            DestinationServer = !string.IsNullOrWhiteSpace(Properties.Settings.Default["DestinationServer"].ToString()) ? Properties.Settings.Default["DestinationServer"].ToString() : DestinationServer;

           
        }

        private ColumnRole[] _columnRoles;
        public IEnumerable<ColumnRole> ColumnRoles
        {
            get
            {
                if (_clicked)
                {
                    try
                    {
                        _columnRoles = _columnRoles ?? GetColumnRoles(ConnectionString).ToArray();
                        return _columnRoles;
                    }
                    catch (Exception e)
                    {
                        MessageBox.Show(e.Message);
                    }
                }

                return Enumerable.Empty<ColumnRole>();
            }
        }



        private KeyValuePair<string, string> _selectedOutput;
        public KeyValuePair<string, string> SelectedOutput
        {
            get
            {
                return _selectedOutput;
            }
            set
            {
                _selectedOutput = value;
                if (PropertyChanged != null) PropertyChanged(this, new PropertyChangedEventArgs("SelectedOutput"));

            }
        }





        private bool _clicked;
        public void Generate()
        {
            try
            {
                HlpNextOffsets = GetFromResources(GetType().Assembly, "SqlDeltaGenerator.CreateHlpNextOffsets.sqlt");
                HlpRequestLog = GetFromResources(GetType().Assembly, "SqlDeltaGenerator.CreateHlpRequestLog.sqlt");
                BufferTable = GenerateBufferCreate(ColumnRoles);
                StagingTable = GenerateStagingCreate(ColumnRoles, Historizing);
                Merge = GenerateHistorizedMerge(ColumnRoles, Historizing);
                SsisScript = GenerateSsisScript(ColumnRoles);
                StandAloneScript = GenerateStandAloneScript(ColumnRoles);
                EnableStep3();
                if (ErrorMessage != null)
                {
                    ErrorMessage = null;
                    this.PropertyChanged(this, new PropertyChangedEventArgs("ErrorMessage"));
                }


            }
            catch (Exception e)
            {

                ErrorMessage = e.Message;
                this.PropertyChanged(this, new PropertyChangedEventArgs("ErrorMessage"));
            }


        }



        public string BufferTable { get; set; }
        public string StagingTable { get; set; }
        public string Merge { get; set; }
        public string SsisScript { get; set; }
        public string StandAloneScript { get; set; }
        public string HlpNextOffsets { get; set; }
        public string HlpRequestLog { get; set; }
        public string ErrorMessage { get; set; }



        public string GenerateHistorizedMerge(IEnumerable<ColumnRole> columnRoless, bool historizing)
        {
            var columnRoles = columnRoless.ToArray();
            var keys = columnRoles.Where(cr => cr.Role.Equals(Key)).Select(cr => cr.ColumnName);
            var attributes = columnRoles.Where(cr => cr.Role.Equals(Attribute)).Select(cr => cr.ColumnName);
            var deltaHande1 = columnRoles.SingleOrDefault(r => r.DeltaHandle);
            var deltaHande = deltaHande1 != null ? deltaHande1.ColumnName : "no";
            var programm = historizing ? GetFromResources(GetType().Assembly, "SqlDeltaGenerator.MergeHistorizing.sqlt") :
                GetFromResources(GetType().Assembly, "SqlDeltaGenerator.MergeUpdate.sqlt");
            string result = Razor.Parse(programm, new
            {
                Keys = keys,
                Attributes = attributes,
                DeltaHandle = deltaHande,
                SourceTable = BufferPrefix + SourceTable,
                TargetTable = StagingPrefix + SourceTable,
                MergeMatched = String.Join(",", attributes.Select(a => string.Format("MyTarget.{0} = SourceTableCte.{0}", a)))
            
            });

            return result;
        }

        public string GenerateBufferCreate(IEnumerable<ColumnRole> columnRoless)
        {
            var keyfields = string.Join(",", columnRoless.Where(r => r.Role.Equals(DeltaGenerator.Key)).Select(r => r.ColumnName));
            var fields = string.Join(",\n", columnRoless.Select(r => r.ColumnName + " " + r.DataType + " "
                + (r.Role.Equals(DeltaGenerator.Key) ? "NOT NULL" : "NULL")
                ));
            var create = string.Format(@"CREATE TABLE [dbo].[BUFFER_{0}]({1}
,CONSTRAINT pk_BUFFER_{0} PRIMARY KEY ({2})
)", SourceTable, fields, keyfields);

            return create;
        }

        public string GenerateStagingCreate(IEnumerable<ColumnRole> columnRoless, bool historizing)
        {

            var keyfields = string.Join(",", columnRoless.Where(r => r.Role.Equals(DeltaGenerator.Key)).Select(r => r.ColumnName));
            if (historizing)
            {
                keyfields += ",_CreatedRequest";
            }
            var fields = string.Join(",\n", columnRoless.Select(r => r.ColumnName + " " + r.DataType + " "
                + (r.Role.Equals(DeltaGenerator.Key) ? "NOT NULL" : "NULL")
                ));
            var create = string.Format(@"CREATE TABLE [dbo].[STAGING_{0}]({1}
, _CreatedRequest int,
_ModifiedRequest int,
 _MostRecentRecord int
,CONSTRAINT pk_STAGING_{0} PRIMARY KEY ({2})
)", SourceTable, fields, keyfields);

            return create;

        }


        private void EnableStep2()
        {
            Step2Enabled = true;
            Step3Enabled = false;
            if (PropertyChanged != null) PropertyChanged(this, new PropertyChangedEventArgs("Step2Enabled"));
            if (PropertyChanged != null) PropertyChanged(this, new PropertyChangedEventArgs("Step3Enabled"));
        }

        private void EnableStep3()
        {
            Step2Enabled = true;
            Step3Enabled = true;
            if (PropertyChanged != null) PropertyChanged(this, new PropertyChangedEventArgs("Step2Enabled"));
            if (PropertyChanged != null) PropertyChanged(this, new PropertyChangedEventArgs("Step3Enabled"));
        }

        public bool Step3Enabled { get; set; }
        public bool Step2Enabled { get; set; }

        public void ResetTable()
        {
            _clicked = true;
            _columnRoles = null;

            //einstellungen speichern
            Properties.Settings.Default["SourceDatabase"] = SourceDatabase;
            Properties.Settings.Default["SourceSchema"] = SourceSchema;
            Properties.Settings.Default["SourceTable"] = SourceTable;
            Properties.Settings.Default["SourceServer"] = SourceServer;
            Properties.Settings.Default["DestinationServer"] = DestinationServer;
            Properties.Settings.Default["DestinationDatabase"] = DestinationDatabase;

            Properties.Settings.Default.Save(); // Saves settings in application configuration file

            if (PropertyChanged != null) PropertyChanged(this, new PropertyChangedEventArgs("ColumnRoles"));

            EnableStep2();
        }

        public IEnumerable<ColumnRole> GetColumnRoles(string connString)
        {


            try
            {


                var result = new List<ColumnRole>();
                using (var sqlConnection1 = new SqlConnection(connString))
                {
                    var cmd = new SqlCommand
                    {
                        CommandText = string.Format(@"with cte as (SELECT
[sCOL].[name] AS [ColumnName]
, CASE
WHEN [sTYP].[name] IN ('char','varchar','nchar','nvarchar','binary','varbinary')
THEN [sTYP].[name] + '(' + CAST([sCOL].[max_length] AS VARCHAR(10)) + ')'
WHEN [sTYP].[name] IN ('float','decimal','numeric','real')
THEN [sTYP].[name] + '(' + CAST([sCOL].[precision] AS VARCHAR(10)) + ',' + CAST([sCOL].[scale] AS VARCHAR(10)) + ')'
ELSE [sTYP].[name]
END AS [DataType]
, CASE [sCOL].[is_nullable]
WHEN 0*1 THEN 'Yes'
ELSE 'No'
END AS [IsNullable]
, CASE
WHEN [IdxDtls].[column_id] IS NOT NULL THEN 'Yes'
ELSE 'No'
END AS [IsPK]
, CASE
WHEN [sFKC].[parent_column_id] IS NOT NULL THEN 'Yes'
ELSE 'No'
END AS [IsFK]
, [sEXP].[value] AS [ColumnDescription]
FROM
[sys].[objects] AS [sOBJ]
INNER JOIN [sys].[columns] AS [sCOL]
ON [sOBJ].[object_id] = [sCOL].[object_id]
LEFT JOIN [sys].[types] AS [sTYP]
ON [sCOL].[user_type_id] = [sTYP].[user_type_id]
LEFT JOIN (
SELECT [sIDX].[object_id], [sIXC].[column_id]
FROM
[sys].[indexes] AS [sIDX]
INNER JOIN [sys].[index_columns] AS [sIXC]
ON [sIDX].[object_id] = [sIXC].[object_id]
AND [sIDX].[index_id] = [sIXC].[index_id]
WHERE [sIDX].[is_primary_key] = 0*1
) AS [IdxDtls]
ON [sCOL].[object_id] = [IdxDtls].[object_id]
AND [sCOL].[column_id] = [IdxDtls].[column_id]
LEFT JOIN [sys].[foreign_key_columns] AS [sFKC]
ON [sCOL].[object_id] = [sFKC].[parent_object_id]
AND [sCOL].[column_id] = [sFKC].[parent_column_id]
LEFT JOIN [sys].[extended_properties] AS [sEXP]
ON [sOBJ].[object_id] = [sEXP].[major_id]
AND [sCOL].[column_id] = [sEXP].[minor_id]
AND [sEXP].[class] = 1
AND [sEXP].[minor_id] > 0
AND [sEXP].[name] = N'MS_Description'
WHERE
[sOBJ].[type] = 'U'
--AND SCHEMA_NAME([sOBJ].[schema_id]) = N'dbo'
AND [sOBJ].[name] = N'{0}'
--ORDER BY [ColumnName] 
)
select distinct columnname,datatype from cte
GO", SourceTable),
                        CommandType = CommandType.Text,
                        Connection = sqlConnection1
                    };

                    sqlConnection1.Open();
                    var reader = cmd.ExecuteReader();
                    var schema = reader.GetSchemaTable();
                    while (reader.Read())
                    {
                        result.Add(new ColumnRole { ColumnName = reader.GetString(0), Role = "Attribute", DataType = reader.GetString(1) });
                    }


                    sqlConnection1.Close();

                }
                if (ErrorMessage != null)
                {
                    ErrorMessage = null;
                    this.PropertyChanged(this, new PropertyChangedEventArgs("ErrorMessage"));
                }

                return result;
            }
            catch (Exception e)
            {

                ErrorMessage = e.Message;
                this.PropertyChanged(this, new PropertyChangedEventArgs("ErrorMessage"));
            }
            return Enumerable.Empty<ColumnRole>();
        }

        public string GenerateSsisScript(IEnumerable<ColumnRole> columnRoless)
        {
            var columnRoles = columnRoless.ToArray();
            var keys = columnRoles.Where(cr => cr.Role.Equals(Key)).Select(cr => cr.ColumnName);
            if (!keys.Any())
            {
                throw new Exception("No Key selected!");
            }
            var attributes = columnRoles.Where(cr => cr.Role.Equals(Attribute)).Select(cr => cr.ColumnName);
            var deltaHande1 = columnRoles.SingleOrDefault(r => r.DeltaHandle);
            if (deltaHande1==null)
            {
                throw new Exception("No Delta Handle selected!");
            }
            var deltaHande = deltaHande1 != null ? deltaHande1.ColumnName : "no";
            var programm = GetFromResources(GetType().Assembly, "SqlDeltaGenerator.SsisScript.txt");
            //string template = "Hello @Model.Name! Welcome to Razor!";
            string result = Razor.Parse(programm, new
            {
                Keys = keys,
                Attributes = attributes,
                DeltaHandle = deltaHande,
                SourceTable = SourceSchema + "." + SourceTable,
                TargetTable = BufferPrefix + SourceTable,
                DestinationServer = DestinationServer,
                SourceServer = SourceServer,
                SourceDb = SourceDatabase,
                DestinationDb = DestinationDatabase
            });
            return result;
            //return string.Join(",", ColumnRoles.Select(cr => cr.ColumnName+" "+cr.Role));
        }

        public string GenerateStandAloneScript(IEnumerable<ColumnRole> columnRoless)
        {
            var columnRoles = columnRoless.ToArray();
            var keys = columnRoles.Where(cr => cr.Role.Equals(Key)).Select(cr => cr.ColumnName);
            var attributes = columnRoles.Where(cr => cr.Role.Equals(Attribute)).Select(cr => cr.ColumnName);
            var deltaHande1 = columnRoles.Single(r => r.DeltaHandle);
            var deltaHande = deltaHande1 != null ? deltaHande1.ColumnName : "no";
            var programm = GetFromResources(GetType().Assembly, "SqlDeltaGenerator.StandAloneScript.txt");
            //string template = "Hello @Model.Name! Welcome to Razor!";
            string result = Razor.Parse(programm, new
            {
                Keys = keys,
                Attributes = attributes,
                DeltaHandle = deltaHande,
                SourceTable = SourceSchema + "." + SourceTable,
                TargetTable = BufferPrefix + SourceTable,
                DestinationServer = DestinationServer,
                SourceServer = SourceServer,
                SourceDb = SourceDatabase,
                DestinationDb = DestinationDatabase
            });
            return result;
        }

        public static string GetFromResources(Assembly assembly, string resourceName)
        {
            using (var stream = assembly.GetManifestResourceStream(resourceName))
            {
                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }



        public event PropertyChangedEventHandler PropertyChanged;




        private string _commandToShow;


        public string CommandToShow
        {
            get { return _commandToShow; }
            set
            {
                _commandToShow = value;
                if (PropertyChanged != null) PropertyChanged(this, new PropertyChangedEventArgs("CommandToShow"));
            }
        }
    }

    public class ColumnRole
    {
        public string ColumnName { get; set; }
        public string Role { get; set; }
        public string DataType { get; set; }
        public bool DeltaHandle { get; set; }

        public IEnumerable<string> Roles
        {
            get
            {
                yield return DeltaGenerator.Attribute;
                yield return DeltaGenerator.Key;
                //yield return DeltaGenerator.DeltaHandle;
                yield return DeltaGenerator.Ignore;
            }
        }
    }
}
