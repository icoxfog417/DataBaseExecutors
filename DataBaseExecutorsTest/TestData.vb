Imports DataBaseExecutors
Imports DataBaseExecutors.Entity
Imports System.Reflection

''' <summary>
''' Make TestData For UnitTest
''' </summary>
''' <remarks></remarks>
Public Class TestData

    Public Const TableName As String = "SALES_ORDER"

    Public Shared Sub Create(ByVal conName As String)
        Dim db As New DBExecution(conName)
        db.importTable(MakeTable)
    End Sub

    Public Shared Sub Drop(ByVal conName As String)
        Dim db As New DBExecution(conName)
        db.sqlExecution("DROP TABLE " + TableName)
    End Sub

    Public Shared Function GetTestData() As List(Of SalesOrder)
        Dim list As New List(Of SalesOrder)


        list.Add(New SalesOrder(1, 1, "Apple", 10.5, New DateTime(2011, 1, 1), New DateTime(2011, 1, 30)))
        list.Add(New SalesOrder(1, 2, "Banana", 5.1, New DateTime(2011, 1, 2), New DateTime(2011, 1, 30)))
        list.Add(New SalesOrder(2, 1, "Apricot", 0.9, New DateTime(2011, 2, 1), New DateTime(2011, 2, 10)))
        list.Add(New SalesOrder(3, 1, "Kiwi", 20.1, New DateTime(2011, 2, 2), New DateTime(2011, 2, 10)))
        list.Add(New SalesOrder(4, 1, "Apple", 10.9, New DateTime(2011, 2, 6), New DateTime(2011, 2, 20)))
        list.Add(New SalesOrder(4, 2, "Cherry", 7.1, New DateTime(2011, 2, 6), New DateTime(2011, 2, 20)))

        Return list

    End Function

    Private Shared Function MakeTable() As DataTable
        Dim table As New DataTable
        Dim model As New DBExecutionTest

        'set table
        table.TableName = TableName
        table.Columns.Add("OrderNo", GetType(Integer))
        table.Columns.Add("OrderDetail", GetType(Integer))
        table.Columns.Add("Material", GetType(String))
        table.Columns.Add("Quan", GetType(Decimal))
        table.Columns.Add("OrderDate", GetType(String))
        table.Columns.Add("DeliverDate", GetType(DateTime))

        'prepare data
        For Each item As SalesOrder In GetTestData()
            Dim row As DataRow = table.NewRow
            row(0) = item.OrderNo
            row(1) = item.OrderDetail
            row(2) = item.MaterialCode
            row(3) = item.Quantity
            row(4) = item.OrderDate.ToString("yyyyMMdd") 'store data by formatted string
            row(5) = item.DeliverDate
            table.Rows.Add(row)
        Next

        Return table

    End Function


End Class

''' <summary>
''' Test Data Class
''' </summary>
''' <remarks></remarks>
<DBSource(Table:="SALES_ORDER")>
Public Class SalesOrder
    Implements IDBEntity

    <DBColumn(IsKey:=True, Order:=1)>
    Public Property OrderNo As Integer = 0
    <DBColumn(IsKey:=True, Order:=2)>
    Public Property OrderDetail As Integer = 0

    <DBColumn(Name:="Material")>
    Public Property MaterialCode As String = ""
    <DBColumn(Name:="Quan")>
    Public Property Quantity As Decimal = 0

    <DBColumn(Format:="yyyyMMdd")>
    Public Property OrderDate As DateTime

    <DBColumn()>
    Public Property DeliverDate As DateTime

    Public Sub New()

    End Sub
    Public Sub New(no As Integer, dno As Integer, m As String, q As Decimal, oYmd As DateTime, dYmd As DateTime)
        Me.OrderNo = no
        Me.OrderDetail = dno
        Me.MaterialCode = m
        Me.Quantity = q
        Me.OrderDate = oYmd
        Me.DeliverDate = dYmd
    End Sub

    Public Function isKeyEqual(ByVal order As SalesOrder) As Boolean
        If Me.OrderNo = order.OrderNo And Me.OrderDetail = order.OrderDetail Then
            Return True
        Else
            Return False
        End If
    End Function

    Public Overrides Function Equals(obj As Object) As Boolean
        If Not TypeOf obj Is SalesOrder Then Return False

        Dim right As SalesOrder = CType(obj, SalesOrder)
        If isKeyEqual(right) And _
            Me.MaterialCode = right.MaterialCode And Me.Quantity = right.Quantity And _
            Me.OrderDate = right.OrderDate And Me.DeliverDate.ToString("yyyyMMddHHmmss") = right.DeliverDate.ToString("yyyyMMddHHmmss") Then
            Return True
        Else
            Return False
        End If

    End Function

    Public Overrides Function ToString() As String
        Dim result = OrderNo.ToString + "-" + OrderDetail.ToString + ":" + MaterialCode + " " + Quantity.ToString + "kg @" + OrderDate.ToString("yyyy/MM/dd") + " - " + DeliverDate.ToString("yyyy/MM/dd")
        Return result
    End Function

End Class