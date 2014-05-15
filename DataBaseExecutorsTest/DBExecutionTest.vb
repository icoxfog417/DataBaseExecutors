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
    Public Sub ImportData()

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

        'prepare data for update
        db.addFilter("pNo", o1.OrderNo)
        db.addFilter("pDetail", o1.OrderDetail)
        db.sqlExecution("INSERT INTO " + TestData.TableName + "(OrderNo,OrderDetail) VALUES ( :pNo,:pDetail) ")

        'set primary key
        table.PrimaryKey = {table.Columns("OrderNo"), table.Columns("OrderDetail")}

        'set optional parameter
        table.Columns("CommentText").ExtendedProperties.Add("UseDefault", True) ' is not include when insert

        'prepare DataTable
        TestData.importList(table, New List(Of SalesOrder) From {o1, o2})

        'execute
        Dim logs As List(Of RowInfo) = db.importTable(table)

        'confirm
        Assert.AreEqual(0, logs.Count)

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
            Assert.AreEqual(defaultDate.ToString("yyyyMMdd"), row("OrderDate").ToString())
            Assert.AreEqual(defaultDate.ToString("yyyyMMddHHmmss"), CDate(row("DeliverDate")).ToString("yyyyMMddHHmmss"))

        Next

        'delete test data
        deleteOrder(db, o1)
        deleteOrder(db, o2)


    End Sub

    <TestMethod()>
    Public Sub ImportDataWithProperty()

        Dim db = New DBExecution(ConnectionName)
        Dim table As DataTable = TestData.toDataTable(Nothing)
        Dim defaultDate As DateTime = New DateTime(1100, 1, 1)
        Dim now As DateTime = DateTime.Now

        'prepare test data
        Dim o As SalesOrder = TestData.createOrder() 'will update
        o.MaterialCode = "Material9000"
        o.Quantity = 10
        o.OrderDate = defaultDate
        o.DeliverDate = defaultDate
        o.CommentText = "Update is Executed"

        'set primary key
        table.PrimaryKey = {table.Columns("OrderNo"), table.Columns("OrderDetail")}

        'set extend option
        table.Columns("OrderDate").AddPropertyForTimeStamp("yyyyMMdd") 'as formated string
        table.Columns("DeliverDate").AddPropertyForTimeStamp() ' as datetime
        table.Columns("CommentText").AddPropertyForIgnore() 'ignore this column

        TestData.importList(table, New List(Of SalesOrder) From {o})

        'execute
        Dim logs As List(Of RowInfo) = db.importTable(table)

        'confirm
        Assert.AreEqual(0, logs.Count)

        Dim stored As DataTable = selectOrder(db, o)

        Assert.IsTrue(now.ToString("yyyyMMdd") <= stored.Rows(0)("OrderDate").ToString)
        Assert.IsTrue(now.ToString("yyyyMMddHHmmss") <= CDate(stored.Rows(0)("DeliverDate")).ToString("yyyyMMddHHmmss"))
        Assert.IsTrue(String.IsNullOrEmpty(stored.Rows(0)("CommentText").ToString))

        deleteOrder(db, o)

    End Sub

    <TestMethod()>
    Public Sub ImportDataWithValidation()

        Dim db = New DBExecution(ConnectionName)
        Dim table As DataTable = TestData.toDataTable(Nothing)
        Dim msg As String = "Quantity is Checked"

        'prepare test data
        Dim o1 As SalesOrder = TestData.createOrder()
        o1.MaterialCode = "MaterialAAA"
        o1.Quantity = 900

        Dim o2 As SalesOrder = TestData.createOrder()
        o2.MaterialCode = "MaterialBBB"
        o2.Quantity = 100

        'set primary key
        table.PrimaryKey = {table.Columns("OrderNo"), table.Columns("OrderDetail")}

        TestData.importList(table, New List(Of SalesOrder) From {o1, o2})

        'execute
        Dim logs As List(Of RowInfo) = db.importTable(table, Function(rowInfo As RowInfo) As RowInfo
                                                                 Dim newInfo As New RowInfo(rowInfo)

                                                                 'Validation
                                                                 If newInfo.RowValues("Quan") > 500 Then
                                                                     newInfo.Messages.Add("Too match quantity!!")
                                                                     newInfo.IsValid = False
                                                                 End If

                                                                 'Convert Value
                                                                 newInfo.RowValues("CommentText") = msg

                                                                 Return newInfo
                                                             End Function)

        'confirm(1 validation error)
        Assert.AreEqual(1, logs.Count)

        Dim stored As DataTable = selectOrder(db, o2)
        Assert.AreEqual(msg, stored.Rows(0)("CommentText"))

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
