Imports Microsoft.VisualBasic

Namespace DataBaseExecutors.Adapter

    ''' <summary>
    ''' Adapter For SqlServer
    ''' </summary>
    ''' <remarks></remarks>
    Public Class SqlParameterAdapter
        Inherits AbsDBParameterAdapter

        Public Overrides ReadOnly Property DBPlaceHolder As String
            Get
                Return "@"
            End Get
        End Property

    End Class

End Namespace
