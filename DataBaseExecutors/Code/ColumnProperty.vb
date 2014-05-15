
Namespace DataBaseExecutors

    ''' <summary>
    ''' Class for describe custom column property
    ''' </summary>
    ''' <remarks></remarks>
    Public Class ColumnProperty

        Public Const TIMESTAMP_FORMAT As String = "AsTimeStamp"
        Public Const USE_DEFAULT As String = "UseDefault"
        Public Const IGNORE As String = "Ignore"

        Public Property Name As String = ""
        Public Property IsKey As Boolean = False
        Public Property IsGenerated As Boolean = False
        Public Property IsIgnore As Boolean = False
        Public Property TimeStampFormat As String = ""

        ''' <summary>
        ''' Constructor is private. use Read method for getting list of ColumnPropery.
        ''' </summary>
        ''' <remarks></remarks>
        Private Sub New()
        End Sub

        ''' <summary>
        ''' Read table column's properties
        ''' </summary>
        ''' <param name="table"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Shared Function Read(ByVal table As DataTable) As List(Of ColumnProperty)
            Dim props As New List(Of ColumnProperty)

            'get primary keys
            Dim keyNames As String() = table.PrimaryKey.Select(Function(p) p.ColumnName).ToArray

            For Each column As DataColumn In (From c As DataColumn In table.Columns Order By c.Ordinal Select c).ToList
                Dim cp As New ColumnProperty
                cp.Name = column.ColumnName
                cp.IsKey = keyNames.Contains(column.ColumnName)
                cp.IsGenerated = column.AutoIncrement OrElse getExtendProperty(Of Boolean)(column, USE_DEFAULT)
                cp.IsIgnore = getExtendProperty(Of Boolean)(column, IGNORE)
                cp.TimeStampFormat = getExtendProperty(Of String)(column, TIMESTAMP_FORMAT)
                props.Add(cp)
            Next

            Return props

        End Function

        ''' <summary>
        ''' Get column extended property
        ''' </summary>
        ''' <typeparam name="T"></typeparam>
        ''' <param name="column"></param>
        ''' <param name="name"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Private Shared Function getExtendProperty(Of T)(ByVal column As DataColumn, ByVal name As String) As T
            If column.ExtendedProperties.Contains(name) Then
                Return CType(column.ExtendedProperties(name), T)
            Else
                Return Nothing
            End If

        End Function

    End Class

End Namespace
