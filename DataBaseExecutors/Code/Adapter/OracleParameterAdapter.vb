Imports Microsoft.VisualBasic
Imports System.Data
Imports System.Data.Common
Imports System.Reflection

Namespace DataBaseExecutors

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
                    'Oracle/xml型はDbTypeでサポートされていないので、特殊処理を入れる
                    ' 'Data.DbType.Xmlではうまく処理できない　http://download.oracle.com/docs/cd/E16338_01/win.112/b62267/featOraCommand.htm#i1007424

                    setProperty(toParam, "OracleDbType", [Enum].Parse(createOracleType("OracleDbType"), "XmlType"))

                Case Data.DbType.Guid
                    setProperty(toParam, "OracleDbType", [Enum].Parse(createOracleType("OracleDbType"), "Raw"))
                    If TypeOf fromParam.Value Is Guid Then
                        toParam.Value = CType(fromParam.Value, Guid).ToByteArray 'Guidはバイト配列に変換する
                    End If
                    toParam.Size = 16 'Oracle上のGUIDはRAW(16)になる

                Case Data.DbType.AnsiString
                    If fromParam.Size > 8000 Then
                        setProperty(toParam, "OracleDbType", [Enum].Parse(createOracleType("OracleDbType"), "Clob"))
                    End If
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
                Case DbType.AnsiString, DbType.AnsiStringFixedLength
                    If IsDBNull(value) Then
                        value = String.Empty 'NULLを長さ0文字列にする
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

        Public Shared Function escapeOracleMsg(ByVal oraMsg As String) As String
            'エラー番号検索

            'エラーメッセージ内で改行を使用していた場合の対応 これだとLFで改行していた場合困ることになるが、
            '今のところLF改行のSplit無しに正確にメッセージが取れる手段がないので（Oracleからのリターンは改行コードがLF）、とりあえずこれで対応。
            '->ORA- を見つけてSplitする手もあるが、きちんと全てにおいて付くのか未検証

            Dim oraMsgCRLFConvert As String = oraMsg.Replace(vbCrLf, vbVerticalTab) '縦タブに変換
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

            result = result.Replace(vbVerticalTab, vbCrLf) '元に戻す
            Return result

        End Function

    End Class

End Namespace
