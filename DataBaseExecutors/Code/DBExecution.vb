Imports Microsoft.VisualBasic
Imports System.Data
Imports System.Data.Common
Imports System.Collections.Generic
Imports System.Configuration
Imports DataBaseExecutors.Adapter

Namespace DataBaseExecutors

    ''' <summary>
    ''' The class for executing SQL or database function.
    ''' </summary>
    ''' <remarks></remarks>
    Public Class DBExecution
        Implements IDisposable

        'To access the database
        Private _factory As DbProviderFactory
        Private _connectionString As String = ""
        Private _dbProvider As String = ""

        'To deal with the peculiarity  of each database(Oracle/SqlServer and so on...)
        Private _dbAdapter As AbsDBParameterAdapter = Nothing

        'To store the error message
        Private _errMessage As String = ""
        Private _rowAffected As Integer = 0

        'To operate the transaction
        Private _keepedConnection As DbConnection = Nothing
        Private _transaction As DbTransaction = Nothing
        Private _isInTransaction As Boolean = False

        'DBExecution supports functional process to recordsets.
        'You can apply the delegate function to each record.

        ''' <summary>
        ''' Delegate function to each records.<br/>
        ''' You can use this delegate for making xml from recordset.
        ''' </summary>
        Public Delegate Function fetchDb(ByVal reader As DbDataReader, ByVal counter As Long) As String

        ''' <summary>
        ''' Delegate function to each records.<br/>
        ''' You can use this delegate for making list of some class from recordset.
        ''' </summary>
        Public Delegate Function fetchDbItem(Of T)(ByVal reader As DbDataReader, ByVal counter As Long) As T

        ''' <summary>
        ''' Callback function for the end of read.
        ''' </summary>
        Public Delegate Sub fetchDbEnd(ByVal result As Boolean, ByVal counter As Long)

        ''' <summary>
        ''' Callback function for converting result to some class. It's invoked when getting result from database function.
        ''' </summary>
        Public Delegate Function executeDbEnd(Of T)(ByRef result As Object) As T

        Private _commandTimeout As Integer = -1
        ''' <summary>
        ''' Timout limit for database access(the time till sql response return).<br/>
        ''' Default is -1, and then apply the default setting.
        ''' </summary>
        Public Property CommandTimeout() As Integer
            Get
                Return _commandTimeout
            End Get
            Set(ByVal value As Integer)
                _commandTimeout = value
            End Set
        End Property

        Private _filter As New Dictionary(Of String, DBExecutionParameter)
        ''' <summary>
        ''' Parameters for sql. it is used in parameter query.
        ''' </summary>
        Public ReadOnly Property Filter() As Dictionary(Of String, DBExecutionParameter)
            Get
                Return _filter
            End Get
        End Property

        ''' <summary>
        ''' Constructor from connection name (in web.config/app.config)
        ''' </summary>
        Public Sub New(ByVal conName As String)
            _dbProvider = ConfigurationManager.ConnectionStrings(conName).ProviderName.ToString
            _factory = DbProviderFactories.GetFactory(_dbProvider)
            _connectionString = ConfigurationManager.ConnectionStrings(conName).ToString()
            loadAdapter(_dbProvider)
        End Sub

        ''' <summary>
        ''' Constructor from connection's dbtype and connection string.
        ''' </summary>
        <Obsolete("Please use New(ByVal conName As String)")> _
        Public Sub New(ByVal dbtype As String, ByVal str As String)
            _dbProvider = dbtype
            _factory = DbProviderFactories.GetFactory(_dbProvider)
            _connectionString = str
            loadAdapter(_dbProvider)
        End Sub

        ''' <summary>
        ''' Load the adapter for each databases.<br/>
        ''' Adapter is used for adapting parameter/sql to each database's peculiarity.<br/>
        ''' </summary>
        ''' <param name="provider">provider's name</param>
        ''' <remarks></remarks>
        Private Sub loadAdapter(ByVal provider As String)

            Try
                Dim pType As ProviderType = ProviderUtil.toProviderType(provider)

                'Same supported database provider is used in connection.
                If pType <> ProviderType.General Then

                    Dim className As String = Me.GetType.Namespace + ".Adapter." + ProviderUtil.GetProviderPrefix(pType) + "ParameterAdapter"
                    Dim classtype As Type = Type.GetType(className)

                    If Not classtype Is Nothing Then
                        Dim instance As Object = Activator.CreateInstance(classtype)
                        _dbAdapter = CType(instance, AbsDBParameterAdapter)
                    End If

                End If

            Catch ex As Exception
                _dbAdapter = Nothing
            End Try

        End Sub

        ''' <summary>
        ''' Get error message. error message is set after executing sql.
        ''' </summary>
        Public Function getErrorMsg() As String
            Return _errMessage
        End Function

        ''' <summary>
        ''' Get count of affected rows.<br/>
        ''' It is taken from the return of ExecuteNonQuery. So the value depends on each database provider's implementation.
        ''' </summary>
        Public Function getRowAffected() As Integer
            Return _rowAffected
        End Function



        ''' <summary>
        ''' Begin the transaction.<br/>
        ''' If transaction is already begun, the transaction commited,and the restart.
        ''' </summary>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function beginTransaction() As Boolean
            initResultVariables()

            Try
                'If transaction is already existed, commit it.
                endTransaction(True)

                _keepedConnection = _factory.CreateConnection
                _keepedConnection.ConnectionString = _connectionString
                _keepedConnection.Open()
                _transaction = _keepedConnection.BeginTransaction
                _isInTransaction = True

            Catch ex As Exception
                _errMessage = ex.Message
            End Try

            Return returnIsSuccessed()

        End Function

        ''' <summary>
        ''' End the transaction.
        ''' </summary>
        ''' <param name="isProcessSuccess">true:commit,false:rollback</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function endTransaction(ByVal isProcessSuccess As Boolean) As Boolean
            initResultVariables()

            If Not _transaction Is Nothing And Not _keepedConnection Is Nothing Then
                Try
                    If isProcessSuccess Then
                        _transaction.Commit()
                    Else
                        _transaction.Rollback()
                    End If
                    _transaction = Nothing

                Catch ex As Exception
                    _errMessage = ex.Message
                Finally
                    If Not _transaction Is Nothing Then
                        _transaction = Nothing
                    End If
                    If Not _keepedConnection Is Nothing Then
                        _keepedConnection.Close()
                        _keepedConnection = Nothing
                    End If
                    _isInTransaction = False
                End Try

            End If

            Return returnIsSuccessed()

        End Function

        ''' <summary>
        ''' Get connection to database.<br/>
        ''' If in the transaction, return the transaction connection.<br/>
        ''' Now connection is open/close for each sql execution except of the transaction.<br/>
        ''' ->Because if connection is not closed for next sql, you have to close connection by yourself, and it it boring(try/catch and close...).<br/>
        '''   Opening the connection is very fast due to database caching or pooling,so I don't think it causes performance issue.
        ''' </summary>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Private Function getConnection() As DbConnection

            Dim tgt_dbcon As DbConnection = Nothing
            If Not _keepedConnection Is Nothing And _isInTransaction = True Then
                tgt_dbcon = _keepedConnection
            Else
                tgt_dbcon = _factory.CreateConnection
                tgt_dbcon.ConnectionString = _connectionString
            End If

            Return tgt_dbcon

        End Function

        ''' <summary>
        ''' Prepare the Dbcommand for sql execution
        ''' </summary>
        ''' <param name="sqlStr"></param>
        ''' <param name="con"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Private Function prepareSqlExecution(ByVal sqlStr As String, ByRef con As DbConnection) As DbCommand

            initResultVariables()
            Dim sql As String = sqlStr
            Dim dbcom As DbCommand = makeDbCommand(sql, con)
            dbcom.CommandText = sql

            If Not _isInTransaction Then
                con.Open()
            Else
                dbcom.Transaction = _transaction
            End If

            Return dbcom

        End Function

        ''' <summary>
        ''' Set the parameters to DbCommand
        ''' </summary>
        ''' <param name="sqlStr"></param>
        ''' <param name="conObj"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Private Function makeDbCommand(ByRef sqlStr As String, ByRef conObj As DbConnection) As DbCommand
            Dim con As DbConnection = conObj
            If con Is Nothing Then
                con = getConnection()
            End If

            Dim dbcom As DbCommand = con.CreateCommand()
            For Each item As KeyValuePair(Of String, DBExecutionParameter) In _filter
                Dim param As DbParameter = dbcom.CreateParameter()

                'Set value to DbParameter by using adapter(considering database specification)
                item.Value.transferData(param, _dbAdapter)
                dbcom.Parameters.Add(param)

                If Not _dbAdapter Is Nothing Then
                    'Replqce the sql parameter placefolder(: or @)
                    sqlStr = _dbAdapter.convertSqlPlaceFolder(sqlStr, param.ParameterName)
                End If

            Next
            If _commandTimeout > 0 Then
                dbcom.CommandTimeout = _commandTimeout
            End If
            Return dbcom

        End Function

        ''' <summary>
        ''' Execute non query(like insert/update)<br/>
        ''' If you want to use parameter query, you have to set parameter by addFilter().
        ''' </summary>
        ''' <param name="sqlStr"></param>
        Public Function sqlExecution(ByVal sqlStr As String) As Boolean

            Dim dbcon As DbConnection = getConnection()

            Try
                Dim dbcom As DbCommand = prepareSqlExecution(sqlStr, dbcon)
                _rowAffected = dbcom.ExecuteNonQuery()

            Catch ex As System.Exception
                _errMessage = ex.Message
            Finally
                If Not dbcon Is Nothing And Not _isInTransaction Then
                    dbcon.Close()
                End If
                _filter.Clear()
            End Try

            Return returnIsSuccessed()

        End Function

        ''' <summary>
        ''' Execute scalar query(get one value by sql)
        ''' </summary>
        ''' <typeparam name="T"></typeparam>
        ''' <param name="sqlstr"></param>
        Public Function sqlReadScalar(Of T)(ByVal sqlstr As String) As T
            Dim dbcon As DbConnection = getConnection()
            Dim result As Object = Nothing
            Dim returnVal As T
            Try
                Dim dbcom As DbCommand = prepareSqlExecution(sqlstr, dbcon)
                result = dbcom.ExecuteScalar
                returnVal = CType(result, T)

            Catch ex As System.Exception
                _errMessage = ex.Message
            Finally
                If Not dbcon Is Nothing And Not _isInTransaction Then
                    dbcon.Close()
                End If
                _filter.Clear()
            End Try

            Return returnVal


        End Function

        ''' <summary>
        ''' Execute select query.<br/>
        ''' Get recordset by DataTable.
        ''' </summary>
        ''' <param name="sqlStr"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function sqlRead(ByVal sqlStr As String) As DataTable
            Dim dbcon As DbConnection = getConnection()
            Dim dt As New DataTable
            Try
                Dim dbcom As DbCommand = prepareSqlExecution(sqlStr, dbcon)
                Using reader As DbDataReader = dbcom.ExecuteReader()
                    dt.Load(reader)
                End Using
            Catch ex As System.Exception
                _errMessage = ex.Message
            Finally
                If Not dbcon Is Nothing And Not _isInTransaction Then
                    dbcon.Close()
                End If
                _filter.Clear()
            End Try

            Return dt

        End Function

        ''' <summary>
        ''' Execute select query.<br/>
        ''' You can set delegate function for handling each record.
        ''' </summary>
        ''' <typeparam name="T"></typeparam>
        ''' <param name="sqlStr">sql statement</param>
        ''' <param name="callback">function that apply each record</param>
        ''' <param name="closer">function that is executed in the end of process</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function sqlRead(Of T)(ByVal sqlStr As String, ByVal callback As fetchDbItem(Of T), Optional ByVal closer As fetchDbEnd = Nothing) As List(Of T)

            Dim result As Boolean = True
            Dim dbcon As DbConnection = getConnection()
            Dim dbItems As New List(Of T)
            Dim counter As Long = 0

            Try
                Dim dbcom As DbCommand = prepareSqlExecution(sqlStr, dbcon)

                Using reader As DbDataReader = dbcom.ExecuteReader()
                    While (reader.Read())
                        dbItems.Add(callback(reader, counter))
                        counter += 1
                    End While
                End Using
            Catch ex As System.Exception
                _errMessage = ex.Message
                result = False
            Finally
                If Not dbcon Is Nothing And Not _isInTransaction Then
                    dbcon.Close()
                End If
                _filter.Clear()
            End Try

            If Not closer Is Nothing Then
                closer(result, counter) 'Note:If exception occurred in read loop,the counter is stopped there.
            End If

            If result = True Then
                Return dbItems
            Else
                Return Nothing
            End If

        End Function

        ''' <summary>
        ''' Execute select query. And it's result is concatenated string of each delegate function's return.
        ''' </summary>
        ''' <param name="sqlStr"></param>
        ''' <param name="callback"></param>
        ''' <param name="closer"></param>
        ''' <param name="separator">separator for each return</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function sqlReadToString(ByVal sqlStr As String, ByVal callback As fetchDbItem(Of String), Optional ByVal closer As fetchDbEnd = Nothing, Optional ByVal separator As String = "") As String
            Dim dblist As List(Of String) = sqlRead(sqlStr, callback, closer)
            Dim result As String = ""

            If Not dblist Is Nothing And getErrorMsg() = "" Then
                Dim counter As Integer = 0
                For Each line As String In dblist
                    If counter < dblist.Count - 1 Then
                        result += line + separator
                    Else
                        result += line
                    End If
                Next
            End If
            Return result

        End Function

        <Obsolete("Please use typed function sqlRead(Of T), or sqlReadToString")> _
        Public Function sqlRead(ByVal sqlStr As String, ByVal callback As fetchDb, Optional ByVal closer As fetchDbEnd = Nothing) As String
            Return sqlReadToString(sqlStr, Function(reader As DbDataReader, counter As Long) As String
                                               Return callback(reader, counter)
                                           End Function, closer
                                               )
        End Function

        ''' <summary>
        ''' Get item value by name (handle DbNull as String.Empty).
        ''' </summary>
        ''' <param name="reader"></param>
        ''' <param name="colName"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        <Obsolete("Please use DbDataReader Extension GetStringOrDefault")> _
        Public Shared Function readItemSafe(ByRef reader As DbDataReader, ByVal colName As String) As String
            Try
                If IsDBNull(reader(colName)) Then
                    Return ""
                Else
                    Return CStr(reader(colName))
                End If
            Catch ex As Exception
                Return ""
            End Try

        End Function

        ''' <summary>
        ''' Get item value by id (handle DbNull as String.Empty).
        ''' </summary>
        ''' <param name="reader"></param>
        ''' <param name="colIndex"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Shared Function readItemSafe(ByRef reader As DbDataReader, ByVal colIndex As Integer) As String
            Try
                If IsDBNull(reader(colIndex)) Then
                    Return ""
                Else
                    Return CStr(reader(colIndex))
                End If
            Catch ex As Exception
                Return ""
            End Try
        End Function

        ''' <summary>
        ''' Judge the existence of record.
        ''' </summary>
        ''' <param name="sql"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function sqlIsExist(ByVal sql As String) As Boolean
            Dim ret As String = sqlReadToString(sql, Function(reader As DbDataReader, counter As Long) As String
                                                         Return "X"
                                                     End Function)

            If ret = "" Then
                Return False
            Else
                Return True
            End If

        End Function

        ''' <summary>
        ''' Execute sql query for one record,and return the value by Dictionary of column's name and value.
        ''' </summary>
        ''' <param name="sql"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function sqlReadOneRow(ByVal sql As String) As Dictionary(Of String, String)
            Dim result As New Dictionary(Of String, String)
            Dim sqlData As DataTable = sqlRead(sql)

            If Not sqlData Is Nothing AndAlso sqlData.Rows.Count > 0 Then
                For i As Integer = 0 To sqlData.Columns.Count - 1
                    Dim colName As String = sqlData.Columns(i).ColumnName 'Exception occurs when the column name duplicate 
                    If Not result.ContainsKey(colName) Then
                        result.Add(colName, sqlData.Rows(0)(i).ToString)
                    End If
                Next
            End If

            Return result

        End Function

        ''' <summary>
        ''' Add query parameter.
        ''' </summary>
        ''' <param name="key">parameter name</param>
        ''' <param name="val">value </param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function addFilter(ByVal key As String, ByVal val As Object) As Boolean
            Dim param As New DBExecutionParameter
            param.ParameterName = key
            param.Value = val
            _filter.Add(key, param)
            Return True
        End Function

        ''' <summary>
        ''' Add query parameter by Dictionary
        ''' </summary>
        ''' <param name="paramDic">Dictionary->key:parameter name,value:parameter value</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function addFilter(ByRef paramDic As Dictionary(Of String, Object)) As Boolean
            For Each item As KeyValuePair(Of String, Object) In paramDic
                addFilter(item.Key, item.Value)
            Next
            Return True
        End Function

        Public Function addFilter(ByRef paramDic As Dictionary(Of String, String)) As Boolean
            If paramDic Is Nothing Then Return False
            addFilter(paramDic.ToDictionary(Of String, Object)(Function(i) i.Key, Function(i) i.Value))
            Return True
        End Function


        ''' <summary>
        ''' Add typed query parameter
        ''' </summary>
        ''' <param name="p">DBExecutionParameter</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function addFilter(ByVal p As DBExecutionParameter) As Boolean
            If p.ParameterName Is Nothing Or p.ParameterName = "" Then
                Return False
            Else
                _filter.Add(p.ParameterName, p)
                Return True

            End If
        End Function

        ''' <summary>
        ''' Add typed query parameter by List
        ''' </summary>
        ''' <param name="paramList"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function addFilter(ByRef paramList As List(Of DBExecutionParameter)) As Boolean
            Dim result As Boolean = True
            For Each item As DBExecutionParameter In paramList
                result = addFilter(item)
                If Not result Then
                    Exit For
                End If
            Next
            Return result
        End Function

        ''' <summary>
        ''' Add result parameter. It means "return of database's function".
        ''' </summary>
        ''' <param name="pname"></param>
        ''' <remarks></remarks>
        Public Sub addResultParam(Optional ByVal pname As String = "RESULT")
            'By default, parameter type is AnsiString
            Dim param As New DBExecutionParameter
            param.ParameterName = pname
            param.Direction = ParameterDirection.ReturnValue
            addFilter(param)

        End Sub

        ''' <summary>
        ''' Clear sql parameters
        ''' </summary>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function clearFilter() As Boolean
            _filter.Clear()
            Return True
        End Function

        ''' <summary>
        ''' Execute database function.<br/>
        ''' If you want to get return value from function, you have to set parameter for return value. There are two way.<br/>
        ''' 1. Use addResultParam<br/>
        ''' 2. Make DBExecutionParameter and set it's Direction to ParameterDirection.ReturnValue. Then add it by addFilter.<br/>
        ''' If return value is special type(can not cast from string simply), you have to use 2 .
        ''' </summary>
        ''' <typeparam name="T"></typeparam>
        ''' <param name="funcName"></param>
        ''' <param name="callback"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function executeDBFunction(Of T)(ByVal funcName As String, Optional ByVal callback As executeDbEnd(Of T) = Nothing) As T
            initResultVariables()
            Dim resultObj As T
            Using dbcon As DbConnection = _factory.CreateConnection
                dbcon.ConnectionString = _connectionString
                Using dbcommand As DbCommand = dbcon.CreateCommand()

                    'Set function name 
                    dbcommand.CommandText = funcName
                    dbcommand.CommandType = CommandType.StoredProcedure

                    'Set timeout paremter(if it's set)
                    If _commandTimeout > 0 Then
                        dbcommand.CommandTimeout = _commandTimeout
                    End If

                    Dim outParam As DbParameter = Nothing 'Parameter for return value
                    Dim inParams As New List(Of DbParameter)
                    For Each item As KeyValuePair(Of String, DBExecutionParameter) In _filter
                        Dim param As DbParameter = dbcommand.CreateParameter()
                        If item.Value.Direction = ParameterDirection.ReturnValue Then
                            item.Value.transferData(param, _dbAdapter)
                            outParam = param
                        Else
                            item.Value.transferData(param, _dbAdapter)
                            inParams.Add(param)
                        End If
                    Next

                    'Result parameter have to be set before any other parameter(especially in Oracle).
                    If Not outParam Is Nothing Then
                        dbcommand.Parameters.Add(outParam)
                    End If

                    For Each p As DbParameter In inParams
                        dbcommand.Parameters.Add(p)
                    Next

                    Try
                        dbcon.Open()
                        dbcommand.ExecuteNonQuery()
                        resultObj = evalResult(outParam, callback)

                    Catch ex As System.Exception
                        _errMessage = ex.Message
                        resultObj = Nothing
                    Finally
                        If Not dbcon Is Nothing And Not _isInTransaction Then
                            dbcon.Close()
                        End If
                        _filter.Clear()
                    End Try

                End Using
            End Using

            Return resultObj

        End Function

        <Obsolete("Please use typed function , executeDBFunction(Of T)")> _
        Public Function executeDBFunction(ByVal funcName As String) As String
            initResultVariables()
            Dim result As String = executeDBFunction(Of String)(funcName)

            If result Is Nothing Then
                If _errMessage <> "" Then
                    Return _errMessage
                Else
                    Return ""
                End If
            Else
                Return result
            End If

        End Function

        ''' <summary>
        ''' Execute database function (simple way).
        ''' </summary>
        ''' <typeparam name="T"></typeparam>
        ''' <param name="funcName"></param>
        ''' <param name="values">function's parameter by ParamArray</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function executeDBFunction(Of T)(ByVal funcName As String, ByVal ParamArray values() As String) As T
            If Not values Is Nothing Then
                initResultVariables()
                _filter.Clear()

                Dim outParam As New DBExecutionParameter
                outParam.ParameterName = "RESULT"
                outParam.Direction = ParameterDirection.ReturnValue
                _filter.Add(outParam.ParameterName, outParam)

                For count = 0 To values.Count - 1
                    Dim inParam As New DBExecutionParameter
                    inParam.ParameterName = "PARAM" & (count + 1)
                    inParam.Value = values(count)
                    _filter.Add(inParam.ParameterName, inParam)
                Next
            End If

            Return executeDBFunction(Of T)(funcName)

        End Function

        <Obsolete("Please use typed function , executeDBFunction(Of T)")> _
        Public Function executeDBFunction(ByVal funcName As String, ByVal ParamArray values() As String) As String
            Dim result As String = executeDBFunction(Of String)(funcName, values)

            If result Is Nothing Then
                If _errMessage <> "" Then
                    Return _errMessage
                Else
                    Return ""
                End If
            Else
                Return result
            End If

        End Function

        ''' <summary>
        ''' Evaluate return parameter from database function.
        ''' </summary>
        ''' <typeparam name="T"></typeparam>
        ''' <param name="result"></param>
        ''' <param name="callback"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Private Function evalResult(Of T)(ByVal result As DbParameter, Optional ByVal callback As executeDbEnd(Of T) = Nothing) As T
            Dim resultObj As T = Nothing

            If Not result Is Nothing Then
                If Not callback Is Nothing Then 'If callback function for handling result parameter,apply it.
                    resultObj = callback(result.Value)
                ElseIf Not _dbAdapter Is Nothing Then 'If Adapter is set, apply it.
                    resultObj = _dbAdapter.GetResult(result, GetType(T))
                Else
                    If IsDBNull(result.Value) Then
                        resultObj = Nothing
                    Else
                        resultObj = result.Value 'Implicit cast
                    End If
                End If
            End If

            Return resultObj

        End Function

        ''' <summary>
        ''' Create(or delete) table in database, and insert all rows in DataTable.
        ''' </summary>
        ''' <param name="isDropCreate">is drop or create table</param>
        ''' <remarks></remarks>
        Public Function createTable(ByVal table As DataTable, Optional ByVal isDropCreate As Boolean = True) As Dictionary(Of Integer, String)
            Dim tableName As String = table.TableName
            Dim log As New Dictionary(Of Integer, String)

            'Set up table
            If isDropCreate Then
                Dim createSql As String = ""
                Dim columns As New List(Of String)

                For Each column As DataColumn In table.Columns
                    Dim colType As String = If(_dbAdapter IsNot Nothing, _dbAdapter.GetDefaultColumnType(column.DataType), String.Empty)
                    If Not String.IsNullOrEmpty(colType) Then
                        columns.Add(column.ColumnName + " " + colType)
                    End If
                Next
                createSql += "CREATE TABLE " + tableName + "(" + String.Join(",", columns) + " )"

                'execute ddls
                sqlExecution("DROP TABLE " + tableName)
                sqlExecution(createSql)
            Else
                sqlExecution("DELETE FROM " + tableName)
            End If

            'Set error message before insert
            If _errMessage <> "" Then
                Throw New Exception(If(isDropCreate, "Failed to create/drop table. ", "Failed to delete records") + " " + _errMessage)
            End If

            'Insert data to table
            For i As Integer = 1 To table.Rows.Count
                Dim row As DataRow = table.Rows(i - 1)
                Dim ins As String = "INSERT INTO " + tableName

                Dim params = From x As DataColumn In table.Columns
                              Let pName As String = ":p" + x.ColumnName
                              Let pValue As Object = row(x.ColumnName)
                              Select x.ColumnName, pName, pValue

                ins += _
                    " ( " + _
                    String.Join(",", params.Select(Function(x) x.ColumnName).ToList) + " ) VALUES ( " + _
                    String.Join(",", params.Select(Function(x) x.pName).ToList) _
                    + " ) "

                Dim paramDic As Dictionary(Of String, Object) = params.ToDictionary(Function(p) p.pName, Function(p) p.pValue)
                addFilter(paramDic)

                If Not sqlExecution(ins) Then log.Add(i, _errMessage)

            Next

            Return log

        End Function

        ''' <summary>
        ''' Import DataTable to database.<br/>
        ''' Key is defined by DataTable.PrimaryKey. It is used by select query and if key match then update else insert.<br/>
        ''' You can set option to DataColumn.ExtendedProperties.<br/>
        ''' * AsTimeStamp : If this property is set , the column's value is set from DateTime.Now. You can set date time format(like "yyyy/MM/dd") .<br/>
        ''' * UseDefault : If you want to use default value when insert , set this property True.<br/>
        ''' * Ignore : If you want to ignore this column , set this property True.<br/>
        ''' <code>
        ''' 'AsTimeStamp
        ''' column.ExtendedProperties.Add("AsTimeStamp",String.Empty) 'treated as Date
        ''' column.ExtendedProperties.Add("AsTimeStamp","yyyy/MM/dd") 'treated as formatted string
        ''' 
        ''' 'UseDefault
        ''' column.ExtendedProperties.Add("UseDefault",True)
        ''' 
        ''' 'Ignore
        ''' column.ExtendedProperties.Add("Ignore",True)
        ''' 
        ''' </code>
        ''' </summary>
        ''' <param name="table"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function importTable(ByVal table As DataTable) As Dictionary(Of Integer, String)
            If table Is Nothing OrElse table.PrimaryKey.Length = 0 Then Throw New Exception("DataTable is Nothing or PrimaryKey is not set")

            Const TIMESTAMP_FORMAT As String = "AsTimeStamp"
            Const USE_DEFAULT As String = "UseDefault"
            Const IGNORE As String = "Ignore"

            Dim tableName As String = table.TableName
            Dim keyNames As String() = table.PrimaryKey.Select(Function(p) p.ColumnName).ToArray
            Dim log As New Dictionary(Of Integer, String)
            Dim expropAsBool = Function(col As DataColumn, exprop As String) As Boolean
                                   Return col.ExtendedProperties.Contains(exprop) AndAlso CBool(col.ExtendedProperties(exprop))
                               End Function

            Dim colDefine = From c As DataColumn In table.Columns
                            Let isKey As Boolean = keyNames.Contains(c.ColumnName)
                            Let isGenerated As Boolean = c.AutoIncrement OrElse expropAsBool(c, USE_DEFAULT)
                            Let isIgnore As Boolean = expropAsBool(c, IGNORE)
                            Let tspFormat As String = If(c.ExtendedProperties.Contains(TIMESTAMP_FORMAT), c.ExtendedProperties(TIMESTAMP_FORMAT).ToString, Nothing)
                            Order By c.Ordinal
                            Select c, isKey, isGenerated, isIgnore, tspFormat

            Dim keyPart = Function(row As DataRow) As String
                              Dim ps As New List(Of String)
                              For i As Integer = 0 To UBound(keyNames)
                                  Dim p As String = ":pKey" + i.ToString
                                  addFilter(p, row(keyNames(i)))
                                  ps.Add(keyNames(i) + " = " + p)
                              Next
                              Dim wherePart As String = String.Join(" AND ", ps)
                              Return wherePart
                          End Function

            For i As Integer = 1 To table.Rows.Count
                Dim row As DataRow = table.Rows(i - 1)

                'Confirm does row exist in database
                Dim rowCount As Integer = sqlReadScalar(Of Integer)("SELECT COUNT(*) FROM " + table.TableName + " WHERE " + keyPart(row))

                If rowCount > 1 Then
                    Throw New Exception("Multiple rows that have same key exist in database.")
                Else

                    Dim targets As New List(Of String)
                    Dim pNames As New List(Of String)
                    For Each col In colDefine
                        Dim isAdd As Boolean = True

                        If rowCount = 0 Then 'insert
                            If col.isGenerated Then isAdd = False
                        Else 'update
                            If col.isKey Then isAdd = False
                        End If
                        If col.isIgnore Then isAdd = False

                        If isAdd Then
                            targets.Add(col.c.ColumnName)
                            pNames.Add(":p" + col.c.ColumnName)
                            If col.tspFormat IsNot Nothing Then 'If column is timestamp 
                                If col.tspFormat = String.Empty Then
                                    addFilter(":p" + col.c.ColumnName, DateTime.Now)
                                Else
                                    addFilter(":p" + col.c.ColumnName, DateTime.Now.ToString(col.tspFormat))
                                End If
                            Else
                                addFilter(":p" + col.c.ColumnName, row(col.c.ColumnName))
                            End If
                        End If

                    Next

                    'Execute sql
                    Dim sql As String = ""
                    If rowCount = 0 Then 'insert
                        sql = "INSERT INTO " + table.TableName + "( " + String.Join(",", targets) + " ) VALUES ( " + String.Join(",", pNames) + " )"

                    Else 'update
                        Dim eachSets As New List(Of String)
                        For t As Integer = 0 To targets.Count - 1
                            eachSets.Add(targets(t) + " = " + pNames(t))
                        Next

                        Dim setPart As String = String.Join(",", eachSets)
                        sql = "UPDATE " + table.TableName + " SET " + setPart + " WHERE " + keyPart(row)

                    End If

                    If Not sqlExecution(sql) Then log.Add(i, _errMessage)

                End If

            Next

            Return log

        End Function


        ''' <summary>
        ''' Initialize variables for result.
        ''' </summary>
        ''' <remarks></remarks>
        Private Sub initResultVariables()
            _errMessage = ""
            _rowAffected = 0
        End Sub

        ''' <summary>
        ''' Return the process is successed or not.
        ''' </summary>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Private Function returnIsSuccessed() As Boolean
            If _errMessage = "" Then
                Return True
            Else
                Return False
            End If
        End Function

        ''' <summary>
        ''' Implements Dispose method.
        ''' </summary>
        ''' <remarks></remarks>
        Public Sub Dispose() Implements IDisposable.Dispose

            If Not _transaction Is Nothing Then
                Try
                    _transaction.Commit() 'Commit remained transaction
                Catch ex As Exception
                End Try
                _transaction.Dispose()
                _transaction = Nothing
            End If

            If Not _keepedConnection Is Nothing Then
                If _keepedConnection.State <> ConnectionState.Closed Then
                    Try
                        _keepedConnection.Close()
                    Catch ex As Exception
                    End Try
                End If
                _keepedConnection.Dispose()
                _keepedConnection = Nothing
            End If

            'Release objects
            _factory = Nothing
            _dbAdapter = Nothing

        End Sub

    End Class

End Namespace
