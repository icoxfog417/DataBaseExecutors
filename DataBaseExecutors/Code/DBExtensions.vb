Imports System.Runtime.CompilerServices
Imports System.Data.Common
Imports System.Reflection

Namespace DataBaseExecutors

    ''' <summary>
    ''' Extension for DataTable,DbDataReader.<br/>
    ''' Supports getting value from DbDataReader.
    ''' </summary>
    ''' <remarks></remarks>
    Public Module DBExtensions

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

        ''' <summary>
        ''' Add TimeStamp Property To DataColumn(for importTable etc)
        ''' <code>
        ''' 'value is set from DateTime.Now, and ToString by format if it is set.
        ''' column.AddPropertyOfTimeStamp(String.Empty)
        ''' column.AddPropertyOfTimeStamp("yyyy/MM/dd")
        ''' </code>
        ''' </summary>
        ''' <param name="column"></param>
        ''' <param name="format"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        <Extension()> _
        Public Function AddPropertyForTimeStamp(ByVal column As DataColumn, ByVal format As String) As DataColumn
            column.ExtendedProperties.Add(ColumnProperty.TIMESTAMP_FORMAT, format)
            Return column
        End Function

        <Extension()> _
        Public Function AddPropertyForTimeStamp(ByVal column As DataColumn) As DataColumn
            column.ExtendedProperties.Add(ColumnProperty.TIMESTAMP_FORMAT, String.Empty)
            Return column
        End Function

        <Extension()> _
        Public Function AddColumnForTimeStamp(ByVal table As DataTable, columnName As String, ByVal format As String) As DataTable
            table.Columns.Add(columnName).AddPropertyForTimeStamp(format)
            Return table
        End Function

        <Extension()> _
        Public Function AddColumnForTimeStamp(ByVal table As DataTable, columnName As String) As DataTable
            table.Columns.Add(columnName).AddPropertyForTimeStamp(String.Empty)
            Return table
        End Function

        ''' <summary>
        ''' Add Column for fixed value(update user name, updated flag, and so on).
        ''' </summary>
        ''' <param name="table"></param>
        ''' <param name="columnName"></param>
        ''' <param name="value"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        <Extension()> _
        Public Function AddColumnForFixedValue(ByVal table As DataTable, columnName As String, ByVal value As Object) As DataTable
            table.Columns.Add(columnName)
            For Each row As DataRow In table.Rows
                row(columnName) = value 'set fixed value
            Next
            Return table
        End Function

        ''' <summary>
        ''' Add UseDefault Property To DataColumn(for importTable etc).<br/>
        ''' If it is set, value of this column in row is ignored when inset(use table default setting), but used when update.
        ''' </summary>
        ''' <param name="column"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        <Extension()> _
        Public Function AddPropertyForUseDefault(ByVal column As DataColumn) As DataColumn
            column.ExtendedProperties.Add(ColumnProperty.USE_DEFAULT, True)
            Return column
        End Function

        ''' <summary>
        ''' Add Ignore Property To DataColumn(for importTable etc).<br/>
        ''' If it is set, value of this column is ignored.
        ''' </summary>
        ''' <param name="column"></param>
        ''' <remarks></remarks>
        <Extension()> _
        Public Function AddPropertyForIgnore(ByVal column As DataColumn) As DataColumn
            column.ExtendedProperties.Add(ColumnProperty.IGNORE, True)
            Return column
        End Function

        <Extension()> _
        Public Function RowInfos(ByVal table As DataTable) As List(Of RowInfo)
            Return RowInfo.Read(table)
        End Function

    End Module

End Namespace
