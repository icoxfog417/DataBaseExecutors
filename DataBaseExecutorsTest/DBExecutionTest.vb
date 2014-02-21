Imports DataBaseExecutors
Imports System.Data.Common

<TestClass()>
Public Class DBExecutionTest

    Private Const ConnectionName As String = "DefaultConnection"

    <ClassInitialize()>
    Public Shared Sub setUp(context As TestContext)
        TestData.Create(ConnectionName)
    End Sub

    <ClassCleanup()>
    Public Shared Sub TearDown()
        TestData.Drop(ConnectionName)
    End Sub

    <TestMethod()>
    Public Sub SqlExecute()
        Dim db = New DBExecution(ConnectionName)
        Dim result As Boolean = False
        Dim isTrue = Sub(r As Boolean, d As DBExecution)
                         Assert.IsTrue(r)
                         Assert.AreEqual(String.Empty, d.getErrorMsg)
                         Assert.AreEqual(1, d.getRowAffected)
                     End Sub

        'Insert
        Dim o As SalesOrder = TestData.createOrder
        db.addFilter("pNo", o.OrderNo)
        db.addFilter("pDetail", o.OrderDetail)
        result = db.sqlExecution("INSERT INTO " + TestData.TableName + "(OrderNo,OrderDetail) VALUES ( :pNo,:pDetail) ")
        isTrue(result, db)

        'Update
        db.addFilter(":pQuan", 30.1)
        db.addFilter("pDate", DateTime.Now)
        db.addFilter("pNo", o.OrderNo)
        db.addFilter("pDetail", o.OrderDetail)
        result = db.sqlExecution("UPDATE " + TestData.TableName + " SET Material = 'MANA KANA' , Quan = :pQuan , DeliverDate = :pd WHERE OrderNo = :pNo AND OrderDetail = :pDetail")
        isTrue(result, db)

        'Delete
        result = deleteOrder(db, o)
        isTrue(result, db)

    End Sub

    <TestMethod()>
    Public Sub SqlRead()

        Dim db = New DBExecution(ConnectionName)
        Dim expected As DataTable = TestData.toDataTable(TestData.GetTestData)

        'Get DataTable
        Dim table As DataTable = db.sqlRead("SELECT * FROM " + TestData.TableName + " ORDER BY OrderNo,OrderDetail")
        Assert.AreEqual(expected.Rows.Count, table.Rows.Count)

        For i As Integer = 0 To expected.Rows.Count - 1
            For Each col As DataColumn In expected.Columns
                Dim val As Object = table.Rows(i)(col.ColumnName)
                If IsDBNull(val) Then
                    Assert.AreEqual(expected.Rows(i)(col.ColumnName), val.ToString)
                Else
                    Assert.AreEqual(expected.Rows(i)(col.ColumnName), val)
                End If
            Next
        Next

    End Sub


    <TestMethod()>
    Public Sub SqlImportData()

        Dim db = New DBExecution(ConnectionName)
        Dim table As DataTable = TestData.toDataTable(Nothing)
        Dim defaultDate As DateTime = New DateTime(1100, 1, 1)

        'prepare test data
        Dim o1 As SalesOrder = TestData.createOrder() 'will update
        o1.MaterialCode = "Material100"
        o1.Quantity = 10
        o1.OrderDate = defaultDate
        o1.DeliverDate = defaultDate
        o1.CommentText = "updated but OrderDate and DeliverDate will be set by Datetime.Now"

        Dim o2 As SalesOrder = TestData.createOrder() 'will insert
        o2.MaterialCode = "Material200"
        o1.Quantity = 20
        o2.OrderDate = defaultDate
        o2.DeliverDate = defaultDate
        o2.CommentText = "comment is set UseDefault so it won't be set"

        db.addFilter("pNo", o1.OrderNo)
        db.addFilter("pDetail", o1.OrderDetail)
        db.sqlExecution("INSERT INTO " + TestData.TableName + "(OrderNo,OrderDetail) VALUES ( :pNo,:pDetail) ")

        'set primary key
        table.PrimaryKey = {table.Columns("OrderNo"), table.Columns("OrderDetail")}

        'set optional parameter
        table.Columns("OrderDate").ExtendedProperties.Add("AsTimeStamp", "yyyyMMdd") 'as formated string
        table.Columns("DeliverDate").ExtendedProperties.Add("AsTimeStamp", String.Empty) ' as datetime
        table.Columns("CommentText").ExtendedProperties.Add("UseDefault", True) ' is not include when insert

        'prepare DataTable
        TestData.importList(table, New List(Of SalesOrder) From {o1, o2})

        'execute
        Dim logs As List(Of String) = db.importTable(table)

        'confirm
        Assert.AreEqual(0, logs.Count)
        logs.ForEach(Sub(r) Assert.AreEqual(String.Empty, r))

        Dim stored As DataTable = selectOrder(db, o1)
        Dim o2Stored As DataTable = selectOrder(db, o2)
        stored.ImportRow(o2Stored.Rows(0))

        For i As Integer = 1 To stored.Rows.Count
            Dim row As DataRow = stored.Rows(i - 1)
            Dim o As SalesOrder = Nothing
            Select Case i
                Case 1 'update
                    o = o1
                    Assert.AreEqual(o.CommentText, row("CommentText"))
                Case 2 'insert
                    o = o2
                    Assert.IsTrue(IsDBNull(row("CommentText")) OrElse String.IsNullOrEmpty(row("CommentText")))
            End Select

            Assert.AreEqual(o.MaterialCode, row("Material"))
            Assert.AreEqual(o.Quantity, row("Quan"))
            Assert.IsTrue("11000101" < row("OrderDate"))
            Assert.IsTrue(defaultDate < row("DeliverDate"))

        Next

        'delete test data
        deleteOrder(db, o1)
        deleteOrder(db, o2)


    End Sub

    Private Function selectOrder(ByVal db As DBExecution, ByVal o As SalesOrder) As DataTable
        db.addFilter("pNo", o.OrderNo)
        db.addFilter("pDetail", o.OrderDetail)
        Return db.sqlRead("SELECT * FROM " + TestData.TableName + " WHERE OrderNo = :pNo AND OrderDetail = :pDetail")

    End Function

    Private Function deleteOrder(ByVal db As DBExecution, ByVal o As SalesOrder) As Boolean
        db.addFilter("pNo", o.OrderNo)
        db.addFilter("pDetail", o.OrderDetail)
        Return db.sqlExecution("DELETE FROM " + TestData.TableName + " WHERE OrderNo = :pNo AND OrderDetail = :pDetail")

    End Function

    <TestMethod()>
    Public Sub SqlReadScalar()

        Dim db = New DBExecution(ConnectionName)

        'get scalar value
        Dim cnt As Integer = db.sqlReadScalar(Of Integer)("SELECT COUNT(*) AS CNT FROM " + TestData.TableName)
        Assert.AreEqual(TestData.GetTestData.Count, cnt)

    End Sub


End Class
