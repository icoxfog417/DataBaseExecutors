Imports Microsoft.VisualBasic

Namespace DataBaseExecutors

    Public Class SqlParameterAdapter
        Inherits AbsDBParameterAdapter

        Public Overrides ReadOnly Property DBPlaceHolder As String
            Get
                Return "@"
            End Get
        End Property

    End Class

End Namespace
