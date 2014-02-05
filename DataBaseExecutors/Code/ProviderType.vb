Imports System.Configuration


Namespace DataBaseExecutors

    ''' <summary>
    ''' The type of database provider(Oracle,SqlServer etc)<br/>
    ''' Supports Oracle,SqlServer now.
    ''' </summary>
    ''' <remarks></remarks>
    Public Enum ProviderType
        Oracle
        SqlServer
        ''' <summary>General Database(no support)</summary>
        General
    End Enum

    ''' <summary>
    ''' Judge database provider from connection string.
    ''' </summary>
    ''' <remarks></remarks>
    Public Class ProviderUtil

        ''' <summary>
        ''' Get provider name in connection string
        ''' </summary>
        ''' <param name="conName"></param>
        Public Shared Function GetRaw(ByVal conName As String) As String
            Return ConfigurationManager.ConnectionStrings(conName).ProviderName.ToString
        End Function

        ''' <summary>
        ''' Get ProviderType from connection name.
        ''' </summary>
        ''' <param name="conName"></param>
        Public Shared Function GetProvider(ByVal conName As String) As ProviderType
            Dim provider As String = ConfigurationManager.ConnectionStrings(conName).ProviderName.ToString           
            Return toProviderType(provider)
        End Function

        ''' <summary>
        ''' Get ProviderType from provider name.
        ''' </summary>
        ''' <param name="provider"></param>
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
        ''' Get Prefix of database provider.<br/>
        ''' </summary>
        ''' <param name="pType"></param>
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
