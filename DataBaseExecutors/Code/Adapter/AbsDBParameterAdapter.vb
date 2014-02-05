Imports Microsoft.VisualBasic
Imports System.Data
Imports System.Data.Common
Imports System.Reflection

Namespace DataBaseExecutors.Adapter

    ''' <summary>
    ''' Abstract class for peculiarity of database<br/>
    ''' The Adapter class which inherits this must put in Adapter folder , and it's naming is <i>DbPrefix</i>ParameterAdapter.<br/>
    ''' DbType and it's prefix is defined in the class <see cref="DataBaseExecutors.ProviderUtil"/>.
    ''' </summary>
    ''' <remarks></remarks>
    Public MustInherit Class AbsDBParameterAdapter

        Public MustOverride ReadOnly Property DBPlaceHolder() As String

        Public Function convertSqlPlaceFolder(ByVal sql As String, ByVal paramName As String) As String

            'Convert placefolder in sql(now, only deal with : or @)
            Dim result As String = sql
            Dim r As New System.Text.RegularExpressions.Regex("(:|@)(" + paramName + "(\s|\s*[\,\)\(])|" + paramName + "$)", System.Text.RegularExpressions.RegexOptions.IgnoreCase)
            Dim m As System.Text.RegularExpressions.Match = r.Match(result)

            While m.Success

                Dim charArray As Char() = m.Value.ToCharArray
                If charArray(0) <> DBPlaceHolder Then 'replace with peculiarity placefolder
                    charArray(0) = DBPlaceHolder
                    Dim convertedParamName As New String(charArray)
                    result = result.Replace(m.Value, convertedParamName)

                End If
                m = m.NextMatch()
            End While

            Return result

        End Function

        Public Overridable Sub SetDbType(ByVal fromParam As DBExecutionParameter, ByRef toParam As DbParameter)
            toParam.DbType = fromParam.DbType
        End Sub

        Public Overridable Sub SetValue(ByVal fromParam As DBExecutionParameter, ByRef toParam As DbParameter)
            toParam.Value = fromParam.Value
        End Sub

        Public Overridable Function GetResult(ByVal result As DbParameter, ByVal rType As Type) As Object
            Return result.Value
        End Function

        Public Overridable Function GetDefaultColumnType(ByVal type As Type) As String
            Dim columnType As String = ""

            Select Case type
                Case GetType(String)
                    columnType = "VARCHAR(1500)"
                Case GetType(Integer), GetType(Int32)
                    columnType = "INTEGER"
                Case GetType(Double)
                    columnType = "NUMERIC(10,5)"
                Case GetType(Decimal)
                    columnType = "NUMERIC(10,5)"
                Case GetType(DateTime)
                    columnType = "DATETIME"
                Case Else
                    columnType = "VARCHAR(1500)"
            End Select

            Return columnType

        End Function

        Protected Sub setProperty(ByRef obj As Object, ByVal propertyName As String, ByVal value As Object)
            obj.GetType().InvokeMember(propertyName, BindingFlags.SetProperty, Nothing, obj, New Object() {value})
        End Sub
        Protected Function getProperty(ByRef obj As Object, ByVal propertyName As String) As Object
            Return obj.GetType().InvokeMember(propertyName, BindingFlags.GetProperty, Nothing, obj, Nothing)
        End Function



    End Class

End Namespace
