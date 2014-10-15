Imports DataBaseExecutors
Imports System.Data.Common

<TestClass()>
Public Class DBExecutionTest

    Private Const ConnectionName As String = "DefaultConnection"

    <TestInitialize()>
    Public Sub setUp()
        TestData.Create(ConnectionName)
    End Sub

    <TestCleanup()>
    Public Sub TearDown()
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
        Dim defaultComment As String = "Default CommentText"

        'prepare test data
        Dim o1 As SalesOrder = TestData.createOrder() 'will insert
        o1.MaterialCode = "Material9000"
        o1.Quantity = 10
        o1.OrderDate = defaultDate
        o1.DeliverDate = defaultDate
        o1.CommentText = "Insert is Executed"


        Dim o2 As SalesOrder = TestData.createOrder() 'will update
        o2.OrderNo = 99
        o2.OrderDetail = 1
        o2.MaterialCode = "Material9990"
        o2.Quantity = 5.8
        o2.OrderDate = defaultDate
        o2.DeliverDate = defaultDate
        o2.CommentText = "Update is Executed"

        'insert row for update
        Dim insertSql = "INSERT INTO " + TestData.TableName +
                        "(OrderNo,OrderDetail,CommentText) values (" +
                         o2.OrderNo.ToString + "," +
                         o2.OrderDetail.ToString + "," +
                         "'" + defaultComment + "')"

        db.sqlExecution(insertSql)

        'set primary key
        table.PrimaryKey = {table.Columns("OrderNo"), table.Columns("OrderDetail")}

        'set extend option
        table.Columns("OrderDate").AddPropertyForTimeStamp("yyyyMMdd") 'as formated string
        table.Columns("DeliverDate").AddPropertyForTimeStamp() ' as datetime
        table.Columns("CommentText").AddPropertyForIgnore() 'ignore this column

        TestData.importList(table, New List(Of SalesOrder) From {o1, o2})

        'execute
        Dim logs As List(Of RowInfo) = db.importTable(table)

        'confirm
        Assert.AreEqual(0, logs.Count)

        Dim stored1 As DataTable = selectOrder(db, o1)  'check insert

        Assert.IsTrue(now.ToString("yyyyMMdd") <= stored1.Rows(0)("OrderDate").ToString)
        Assert.IsTrue(now.ToString("yyyyMMddHHmmss") <= CDate(stored1.Rows(0)("DeliverDate")).ToString("yyyyMMddHHmmss"))
        Assert.IsTrue(String.IsNullOrEmpty(stored1.Rows(0)("CommentText").ToString))


        Dim stored2 As DataTable = selectOrder(db, o2)  'check update

        Assert.IsTrue(o2.MaterialCode = stored2.Rows(0)("Material").ToString)
        Assert.IsTrue(now.ToString("yyyyMMdd") <= stored2.Rows(0)("OrderDate").ToString)
        Assert.IsTrue(now.ToString("yyyyMMddHHmmss") <= CDate(stored2.Rows(0)("DeliverDate")).ToString("yyyyMMddHHmmss"))
        Assert.IsTrue(defaultComment = stored2.Rows(0)("CommentText").ToString)

        deleteOrder(db, o1)
        deleteOrder(db, o2)

    End Sub

    <TestMethod()>
    Public Sub ImportDataWithValidation()

        Dim db = New DBExecution(ConnectionName)
        Dim table As DataTable = TestData.toDataTable(Nothing)
        Dim validator As New TestRowValidator
        validator.CheckMessage = "the row is checked!"
        validator.RowOffset = 3

        'prepare test data
        Dim o1 As SalesOrder = TestData.createOrder()
        o1.MaterialCode = "MaterialAAA"
        o1.Quantity = 900 'validation error

        Dim o2 As SalesOrder = TestData.createOrder()
        o2.MaterialCode = "MaterialBBB"
        o2.Quantity = 100

        'set primary key
        table.PrimaryKey = {table.Columns("OrderNo"), table.Columns("OrderDetail")}

        TestData.importList(table, New List(Of SalesOrder) From {o1, o2})

        'Test Setup
        Dim logs As List(Of RowInfo) = db.importTable(table, validator)
        Assert.AreEqual(-1, logs.First.Index)

        'Test Validate
        validator.IsPrevent = False
        logs = db.importTable(table, validator)
        Assert.AreEqual(1, logs.Count)

        Dim stored As DataTable = selectOrder(db, o2)
        Assert.AreEqual(validator.CheckMessage, stored.Rows(0)("CommentText"))
        Assert.AreEqual(validator.RowOffset, logs.First.Index)

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

    Public Class TestRowValidator
        Inherits RowInfoValidator

        Public Property RowOffset As Integer = 0
        Public Property CheckMessage As String = ""
        Public Property IsPrevent As Boolean = True

        Public Overrides Function SetUp(ByRef db As DBExecution, ByRef table As DataTable) As List(Of RowInfo)
            If IsPrevent Then
                Dim dummy As New RowInfo(table.Rows(0))
                dummy.Messages.Add("Prevented!")
                dummy.Index = -1
                Return New List(Of RowInfo) From {dummy}
            End If
            Return MyBase.SetUp(db, table)
        End Function

        Public Overrides Function Validate(rowInfo As RowInfo) As RowInfo
            Dim newInfo As New RowInfo(rowInfo)

            'Validation
            If newInfo.RowValues("Quan") > 500 Then
                newInfo.Messages.Add("Too match quantity!!")
                newInfo.IsValid = False
            End If

            'Convert Value
            newInfo.RowValues("CommentText") = CheckMessage

            Return newInfo
        End Function

        Public Overrides Function TearDown(ByRef db As DBExecution, log As List(Of RowInfo)) As List(Of RowInfo)
            log.ForEach(Sub(r) r.Index += RowOffset)
            Return log
        End Function

    End Class

End Class
