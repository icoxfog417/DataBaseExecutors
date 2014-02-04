Imports Microsoft.VisualBasic
Imports System.Data
Imports System.Data.Common
Imports System.Reflection

'メソッドの実装を伴うためテンプレートクラスにしても良いが、そうするとMustOverrideが使えずPlaceFolderの
'実装を強制できないため、Abstractで作成する

Namespace DataBaseExecutors

    Public MustInherit Class AbsDBParameterAdapter

        Public MustOverride ReadOnly Property DBPlaceHolder() As String

        Public Function convertSqlPlaceFolder(ByVal sql As String, ByVal paramName As String) As String

            'プレースフォルダ(一般的に:か@、ODBCの?は名称つきパラメータが使えないので考慮対象外)で始まり、1文字以上の空白が終端で区切られるものを置換
            Dim result As String = sql
            Dim r As New System.Text.RegularExpressions.Regex("(:|@)(" + paramName + "(\s|\s*[\,\)\(])|" + paramName + "$)", System.Text.RegularExpressions.RegexOptions.IgnoreCase)
            Dim m As System.Text.RegularExpressions.Match = r.Match(result)

            While m.Success

                Dim charArray As Char() = m.Value.ToCharArray
                If charArray(0) <> DBPlaceHolder Then '一文字目がプレースフォルダ これがDB固有のものと等しくない場合、置き換える
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

        Public Sub setProperty(ByRef obj As Object, ByVal propertyName As String, ByVal value As Object)
            obj.GetType().InvokeMember(propertyName, BindingFlags.SetProperty, Nothing, obj, New Object() {value})
        End Sub
        Public Function getProperty(ByRef obj As Object, ByVal propertyName As String) As Object
            Return obj.GetType().InvokeMember(propertyName, BindingFlags.GetProperty, Nothing, obj, Nothing)
        End Function

    End Class

End Namespace
