Imports System.Configuration


Namespace DataBaseExecutors

    ''' <summary>
    ''' データベースのプロバイダ(Oracle,SqlServer etc)種別<br/>
    ''' 現在、個別対応を行っているのはOracle/SqlServer
    ''' </summary>
    ''' <remarks></remarks>
    Public Enum ProviderType
        Oracle
        SqlServer
        ''' <summary>一般的なDB(特別な処理がないもの)</summary>
        General
    End Enum

    ''' <summary>
    ''' 接続文字列から、データベースプロバイダの情報を抽出するユーティリティ関数
    ''' </summary>
    ''' <remarks></remarks>
    Public Class ProviderUtil

        ''' <summary>
        ''' プロバイダの生の文字列を取得
        ''' </summary>
        ''' <param name="conName">接続文字列</param>
        Public Shared Function GetRaw(ByVal conName As String) As String
            Return ConfigurationManager.ConnectionStrings(conName).ProviderName.ToString
        End Function

        ''' <summary>
        ''' プロバイダー種別を取得
        ''' </summary>
        ''' <param name="conName">接続文字列</param>
        Public Shared Function GetProvider(ByVal conName As String) As ProviderType
            Dim provider As String = ConfigurationManager.ConnectionStrings(conName).ProviderName.ToString           
            Return toProviderType(provider)
        End Function

        ''' <summary>
        ''' プロバイダー種別を取得
        ''' </summary>
        ''' <param name="provider">プロバイダー文字列</param>
        Public Shared Function toProviderType(ByVal provider As String) As ProviderType
            Dim result As ProviderType = ProviderType.General

            Select Case provider
                Case "Oracle.DataAccess.Client"
                    result = ProviderType.Oracle
                Case "System.Data.SqlClient"
                    result = ProviderType.SqlServer
            End Select

            Return result

        End Function

        ''' <summary>
        ''' クラスの接頭辞を取得<br/>
        ''' 特にSqlServerの場合Sqlなる。
        ''' </summary>
        ''' <param name="pType">プロバイダー種別</param>
        Public Shared Function GetProviderPrefix(ByVal pType As ProviderType) As String
            Dim result As String = ""

            Select Case pType
                Case ProviderType.Oracle
                    result = "Oracle"
                Case ProviderType.SqlServer
                    result = "Sql"
            End Select

            Return result

        End Function

    End Class

End Namespace
