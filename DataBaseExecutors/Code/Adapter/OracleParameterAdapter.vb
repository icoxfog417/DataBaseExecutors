Imports Microsoft.VisualBasic
Imports System.Data
Imports System.Data.Common
Imports System.Reflection

Namespace DataBaseExecutors.Adapter

    ''' <summary>
    ''' Adapter For Oracle
    ''' </summary>
    ''' <remarks></remarks>
    Public Class OracleParameterAdapter
        Inherits AbsDBParameterAdapter

        Private Shared oracleDataAccess As Assembly = Nothing

        Public Overrides ReadOnly Property DBPlaceHolder As String
            Get
                Return ":"
            End Get
        End Property

        Public Sub New()
            If oracleDataAccess Is Nothing Then
                oracleDataAccess = AppDomain.CurrentDomain.GetAssemblies.LastOrDefault(Function(assem) assem.FullName.StartsWith("Oracle.DataAccess"))
            End If
        End Sub

        Public Overrides Sub SetDbType(fromParam As DBExecutionParameter, ByRef toParam As System.Data.Common.DbParameter)

            Select Case fromParam.DbType
                Case Data.DbType.Xml
                    'Oracle XML Type is not supported in DbType,so handle it.
                    'refer http://download.oracle.com/docs/cd/E16338_01/win.112/b62267/featOraCommand.htm#i1007424

                    setProperty(toParam, "OracleDbType", [Enum].Parse(createOracleType("OracleDbType"), "XmlType"))

                Case Data.DbType.Guid
                    setProperty(toParam, "OracleDbType", [Enum].Parse(createOracleType("OracleDbType"), "Raw"))
                    If TypeOf fromParam.Value Is Guid Then
                        toParam.Value = CType(fromParam.Value, Guid).ToByteArray 'Guid convert to Byte Array
                    End If
                    toParam.Size = 16 'Oracle's GUID is RAW(16)

                Case Data.DbType.AnsiString
                    If fromParam.Size > 8000 Then
                        setProperty(toParam, "OracleDbType", [Enum].Parse(createOracleType("OracleDbType"), "Clob"))
                    End If

                Case Data.DbType.String
                    setProperty(toParam, "OracleDbType", [Enum].Parse(createOracleType("OracleDbType"), "NVarchar2"))

                Case Data.DbType.StringFixedLength
                    setProperty(toParam, "OracleDbType", [Enum].Parse(createOracleType("OracleDbType"), "NChar"))

                Case Else
                    'デフォルト動作
                    MyBase.SetDbType(fromParam, toParam)
            End Select

        End Sub

        Public Overrides Function GetResult(result As DbParameter, ByVal rType As Type) As Object
            Dim value As Object = MyBase.GetResult(result, rType)

            Select Case getProperty(result, "OracleDbType")
                Case [Enum].Parse(createOracleType("OracleDbType"), "XmlType")
                    Select Case rType
                        Case GetType(System.Xml.XmlDocument)
                            value = value.GetXmlDocument()
                    End Select
                Case [Enum].Parse(createOracleType("OracleDbType"), "Raw")
                    If rType Is GetType(Guid) Then
                        Dim tobyte As Byte() = value.Value
                        value = New Guid(tobyte)
                    End If
                Case [Enum].Parse(createOracleType("OracleDbType"), "Clob")
                    value = result.Value.value
            End Select

            Select Case result.DbType
                Case DbType.AnsiString, DbType.AnsiStringFixedLength, Data.DbType.String, Data.DbType.StringFixedLength
                    If IsDBNull(value) Then
                        value = String.Empty 'NULL is length 0 string
                    End If
            End Select

            If IsDBNull(value) Then
                value = Nothing
            End If

            Return value

        End Function

        Private Function createOracleType(ByVal typeName As String) As Type
            If Not oracleDataAccess Is Nothing Then
                Return oracleDataAccess.GetTypes().SingleOrDefault(Function(t) t.Name = typeName)
            Else
                Return Nothing
            End If
        End Function

        Public Overrides Function GetDefaultColumnType(type As Type) As String
            Dim columnType As String = ""

            'Caution, Oracle 
            Select Case type
                Case GetType(String)
                    columnType = "VARCHAR2(1500)"
                Case GetType(Int16)
                    columnType = "NUMBER(4)"
                Case GetType(Integer), GetType(Int32)
                    columnType = "NUMBER(9)"
                Case GetType(Int64)
                    columnType = "NUMBER(18)"
                Case GetType(Double)
                    columnType = "NUMBER(10,7)"
                Case GetType(Decimal)
                    columnType = "NUMBER(18,10)"
                Case GetType(DateTime)
                    columnType = "DATE"
                Case Else
                    columnType = "VARCHAR2(1500)"
            End Select

            Return columnType

        End Function

        ''' <summary>
        ''' To handle Oracle's error message.<br/>
        ''' 
        ''' </summary>
        ''' <param name="oraMsg"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Shared Function escapeOracleMsg(ByVal oraMsg As String) As String

            Dim oraMsgCRLFConvert As String = oraMsg.Replace(vbCrLf, vbVerticalTab)
            Dim msgList As String() = Split(oraMsgCRLFConvert, vbLf)
            Dim result As String = ""
            Dim firstPls As Boolean = False

            Dim regex As New System.Text.RegularExpressions.Regex("ORA-\d{5}:|PLS-\d{5}:")

            If msgList.Count > 0 Then
                For Each msg As String In msgList
                    Dim index As Integer = 0
                    Dim match As System.Text.RegularExpressions.Match = regex.Match(msg)

                    While match.Success
                        Dim position As Integer = msg.IndexOf(match.Value) + match.Value.Length
                        If position > index Then
                            index = position
                        End If
                        If Not firstPls And match.Value.StartsWith("PLS-") Then
                            firstPls = True
                        End If
                        match = match.NextMatch()
                    End While

                    If String.IsNullOrEmpty(result) Or firstPls Then
                        If index > 0 Then
                            result = msg.Substring(index)
                        Else
                            result = msg
                        End If
                    End If

                Next

            End If

            result = result.Replace(vbVerticalTab, vbCrLf) 'reverce vertical tab to crlf
            Return result

        End Function

    End Class

End Namespace
