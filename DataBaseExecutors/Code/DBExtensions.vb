Imports System.Runtime.CompilerServices
Imports System.Data.Common
Imports System.Reflection

Namespace DataBaseExecutors

    ''' <summary>
    ''' Extension for DbDataReader.<br/>
    ''' Supports getting value from DbDataReader.
    ''' </summary>
    ''' <remarks></remarks>
    Public Module DbDataReaderExtension

        <Extension()> _
        Public Function GetDecimalOrDefault(ByVal reader As DbDataReader, ByVal name As String) As Decimal
            If IsDBNull(reader(name)) OrElse String.IsNullOrEmpty(reader(name)) Then
                Return 0
            Else
                Dim d As Decimal = 0
                If Decimal.TryParse(reader(name).ToString, d) Then Return d Else Return 0
            End If
        End Function

        <Extension()> _
        Public Function GetIntegerOrDefault(ByVal reader As DbDataReader, ByVal name As String) As Integer
            If IsDBNull(reader(name)) OrElse String.IsNullOrEmpty(reader(name)) Then
                Return 0
            Else
                Dim i As Integer = 0
                If Integer.TryParse(reader(name).ToString, i) Then Return i Else Return 0
            End If
        End Function

        <Extension()> _
        Public Function GetStringOrDefault(ByVal reader As DbDataReader, ByVal name As String) As String
            If IsDBNull(reader(name)) OrElse String.IsNullOrEmpty(reader(name)) Then
                Return String.Empty
            Else
                Return reader(name).ToString
            End If
        End Function

    End Module

End Namespace
