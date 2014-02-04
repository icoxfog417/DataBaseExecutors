Imports Microsoft.VisualBasic
Imports System.Data
Imports System.Data.Common
Imports System.Collections.Generic
Imports System.Configuration

Namespace DataBaseExecutors

    Public Class DBExecution
        Implements IDisposable

        Private _factory As DbProviderFactory
        Private _connectionString As String = ""
        Private _dbProvider As String = ""
        Private _dbAdapter As AbsDBParameterAdapter = Nothing

        Private _errMessage As String = ""
        Private _rowAffected As Integer = 0

        'トランザクション処理用
        Private _trnConnection As DbConnection = Nothing
        Private _transaction As DbTransaction = Nothing
        Private _isInTransaction As Boolean = False

        'レコード処理用Delegate
        Public Delegate Function fetchDb(ByVal reader As DbDataReader, ByVal counter As Long) As String
        Public Delegate Function fetchDbItem(Of T)(ByVal reader As DbDataReader, ByVal counter As Long) As T
        Public Delegate Sub fetchDbEnd(ByVal result As Boolean, ByVal counter As Long)
        Public Delegate Function executeDbEnd(Of T)(ByRef result As Object) As T

        'プロパティ
        Private _commandTimeout As Integer = -1
        Public Property CommandTimeout() As Integer
            Get
                Return _commandTimeout
            End Get
            Set(ByVal value As Integer)
                _commandTimeout = value
            End Set
        End Property

        Private _filter As New Dictionary(Of String, DBExecutionParameter)
        Public ReadOnly Property Filter() As Dictionary(Of String, DBExecutionParameter)
            Get
                Return _filter
            End Get
        End Property

        Public Sub New(ByVal conName As String)
            _dbProvider = ConfigurationManager.ConnectionStrings(conName).ProviderName.ToString
            _factory = DbProviderFactories.GetFactory(_dbProvider)
            _connectionString = ConfigurationManager.ConnectionStrings(conName).ToString()
            loadAdapter(_dbProvider)
        End Sub

        <Obsolete("New(ByVal conName As String)を使用してください")> _
        Public Sub New(ByVal dbtype As String, ByVal str As String)
            _dbProvider = dbtype
            _factory = DbProviderFactories.GetFactory(_dbProvider)
            _connectionString = str
            loadAdapter(_dbProvider)
        End Sub

        Private Sub loadAdapter(ByVal provider As String)
            'ConnectionStringのProviderに応じて、アダプタークラスをロードする
            'アダプタークラスは、OracleParameterAdapter のように、DB種別+Adapterとなるように作成する

            Try
                Dim pType As ProviderType = ProviderUtil.toProviderType(provider)

                If pType <> ProviderType.General Then
                    Dim className As String = Me.GetType.Namespace + "." + ProviderUtil.GetProviderPrefix(pType) + "ParameterAdapter"
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

        Public Function getErrorMsg() As String
            Return _errMessage
        End Function
        Public Function getRowAffected() As Integer
            Return _rowAffected
        End Function
        Public Function beginTransaction() As Boolean
            _errMessage = ""
            _rowAffected = 0
            Try
                'トランザクションがすでにあった場合、一旦終了
                endTransaction(True)

                _trnConnection = _factory.CreateConnection
                _trnConnection.ConnectionString = _connectionString
                _trnConnection.Open()
                _transaction = _trnConnection.BeginTransaction
                _isInTransaction = True

            Catch ex As Exception
                _errMessage = ex.Message
            End Try

            Return returnResult()

        End Function
        Public Function endTransaction(ByVal isProcessSuccess As Boolean) As Boolean
            _errMessage = ""
            _rowAffected = 0
            If Not _transaction Is Nothing And Not _trnConnection Is Nothing Then
                Try
                    If isProcessSuccess Then    '成功：Commit
                        _transaction.Commit()
                    Else            '失敗：Rollback
                        _transaction.Rollback()
                    End If
                    _transaction = Nothing

                Catch ex As Exception
                    _errMessage = ex.Message
                Finally
                    If Not _transaction Is Nothing Then
                        _transaction = Nothing
                    End If
                    If Not _trnConnection Is Nothing Then
                        _trnConnection.Close()
                        _trnConnection = Nothing
                    End If
                    _isInTransaction = False
                End Try

            End If

            Return returnResult()

        End Function
        Private Function getConnection() As DbConnection
            Dim tgt_dbcon As DbConnection = Nothing
            If Not _trnConnection Is Nothing And _isInTransaction = True Then
                tgt_dbcon = _trnConnection
            Else
                tgt_dbcon = _factory.CreateConnection
                tgt_dbcon.ConnectionString = _connectionString
            End If

            Return tgt_dbcon

        End Function
        Private Function prepareSqlExecution(ByVal sqlStr As String, ByRef con As DbConnection) As DbCommand
            'パラメーター初期化
            initResultVariables()
            Dim sql As String = sqlStr
            Dim dbcom As DbCommand = makeDbCommand(sql, con)
            dbcom.CommandText = sql

            'トランザクションセット
            If Not _isInTransaction Then
                con.Open()
            Else
                dbcom.Transaction = _transaction
            End If

            Return dbcom

        End Function
        Private Function makeDbCommand(ByRef sqlStr As String, Optional ByRef conObj As DbConnection = Nothing) As DbCommand
            Dim con As DbConnection = conObj
            If con Is Nothing Then
                con = getConnection()
            End If

            Dim dbcom As DbCommand = con.CreateCommand()
            For Each item As KeyValuePair(Of String, DBExecutionParameter) In _filter
                Dim param As DbParameter = dbcom.CreateParameter()
                item.Value.transferData(param, _dbAdapter)
                dbcom.Parameters.Add(param)

                If Not _dbAdapter Is Nothing Then
                    sqlStr = _dbAdapter.convertSqlPlaceFolder(sqlStr, param.ParameterName)
                End If

            Next
            If _commandTimeout > 0 Then
                dbcom.CommandTimeout = _commandTimeout
            End If
            Return dbcom

        End Function

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

            Return returnResult()

        End Function

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

            '処理終了前の処理が設定されていた場合、実行
            If Not closer Is Nothing Then
                closer(result, counter) '※例外が発生した場合counterはそのレコードの時点で止まっている
            End If

            If result = True Then
                Return dbItems
            Else
                Return Nothing
            End If

        End Function
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

        <Obsolete("今後はsqlRead(Of T)か、結果セットを連結文字列にする場合はsqlReadToStringを使用してください。")> _
        Public Function sqlRead(ByVal sqlStr As String, ByVal callback As fetchDb, Optional ByVal closer As fetchDbEnd = Nothing) As String
            Return sqlReadToString(sqlStr, Function(reader As DbDataReader, counter As Long) As String
                                               Return callback(reader, counter)
                                           End Function, closer
                                               )
        End Function
        Public Shared Function readItemSafe(ByRef reader As DbDataReader, ByVal colName As String) As String
            Try
                If IsDBNull(reader(colName)) Then
                    Return ""
                Else
                    Return reader(colName)
                End If
            Catch ex As Exception
                Return ""
            End Try

        End Function
        Public Shared Function readItemSafe(ByRef reader As DbDataReader, ByVal colIndex As Integer) As String
            Try
                If IsDBNull(reader(colIndex)) Then
                    Return ""
                Else
                    Return reader(colIndex)
                End If
            Catch ex As Exception
                Return ""
            End Try
        End Function
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

        Public Function sqlReadOneRow(ByVal sql As String) As Dictionary(Of String, String)
            Dim result As New Dictionary(Of String, String)
            Dim sqlData As DataTable = sqlRead(sql)

            If Not sqlData Is Nothing AndAlso sqlData.Rows.Count > 0 Then
                For i As Integer = 0 To sqlData.Columns.Count - 1
                    Dim colName As String = sqlData.Columns(i).ColumnName '列項目名が重複する場合Exceptionが発生する
                    If Not result.ContainsKey(colName) Then
                        result.Add(colName, sqlData.Rows(0)(i).ToString)
                    End If
                Next
            End If

            Return result

        End Function

        Public Function addFilter(ByVal key As String, ByVal val As String) As Boolean
            Dim param As New DBExecutionParameter
            param.ParameterName = key
            param.Value = val
            _filter.Add(key, param)
            Return True
        End Function
        Public Function addFilter(ByRef paramDic As Dictionary(Of String, String)) As Boolean
            For Each item As KeyValuePair(Of String, String) In paramDic
                addFilter(item.Key, item.Value)
            Next
            Return True
        End Function
        Public Function addFilter(ByVal p As DBExecutionParameter) As Boolean
            If p.ParameterName Is Nothing Or p.ParameterName = "" Then
                Return False
            Else
                _filter.Add(p.ParameterName, p)
                Return True

            End If
        End Function
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

        Public Sub addResultParam(Optional ByVal pname As String = "RESULT")
            Dim param As New DBExecutionParameter
            param.ParameterName = pname
            param.Direction = ParameterDirection.ReturnValue
            addFilter(param)

        End Sub

        Public Function clearFilter() As Boolean
            _filter.Clear()
            Return True
        End Function

        'function実行はトランザクション対象外
        Public Function executeDBFunction(Of T)(ByVal funcName As String, Optional ByVal callback As executeDbEnd(Of T) = Nothing) As T
            initResultVariables()
            Dim resultObj As T
            Using dbcon As DbConnection = _factory.CreateConnection
                dbcon.ConnectionString = _connectionString
                Using dbcommand As DbCommand = dbcon.CreateCommand()
                    dbcommand.CommandText = funcName
                    dbcommand.CommandType = CommandType.StoredProcedure
                    If _commandTimeout > 0 Then
                        dbcommand.CommandTimeout = _commandTimeout
                    End If
                    Dim outParam As DbParameter = Nothing
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

        <Obsolete("executeDBFunction(Of T)を使用してください")> _
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

        Public Function executeDBFunction(Of T)(ByVal funcName As String, ByVal ParamArray values() As String) As T
            If Not values Is Nothing Then
                initResultVariables()
                _filter.Clear()

                '※Ansi系 = CHAR/VARCHAR (CHARの場合FixedLength) / String系 = NCHAR/NVARCHAR?
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

        <Obsolete("executeDBFunction(Of T)を使用してください")> _
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

        Private Function evalResult(Of T)(ByVal result As DbParameter, Optional ByVal callback As executeDbEnd(Of T) = Nothing) As T
            Dim resultObj As T = Nothing

            If Not result Is Nothing Then
                If Not callback Is Nothing Then
                    resultObj = callback(result.Value)
                ElseIf Not _dbAdapter Is Nothing Then
                    resultObj = _dbAdapter.GetResult(result, GetType(T))
                Else
                    If IsDBNull(result.Value) Then
                        resultObj = Nothing
                    Else
                        resultObj = result.Value '暗黙キャストに任せる
                    End If
                End If
            End If

            Return resultObj

        End Function

        Private Sub initResultVariables()
            _errMessage = ""
            _rowAffected = 0
        End Sub

        Private Function returnResult() As Boolean
            If _errMessage = "" Then
                Return True
            Else
                Return False
            End If
        End Function


        Public Sub Dispose() Implements IDisposable.Dispose

            If Not _transaction Is Nothing Then
                Try
                    _transaction.Commit() 'rollbackとどちらにするか迷いどころだが・・・
                Catch ex As Exception
                End Try
                _transaction.Dispose()
                _transaction = Nothing
            End If

            If Not _trnConnection Is Nothing Then
                If _trnConnection.State <> ConnectionState.Closed Then
                    Try
                        _trnConnection.Close()
                    Catch ex As Exception
                    End Try
                End If
                _trnConnection.Dispose()
                _trnConnection = Nothing
            End If

            '重そうなオブジェクトを解放
            _factory = Nothing
            _dbAdapter = Nothing

        End Sub

    End Class

End Namespace
