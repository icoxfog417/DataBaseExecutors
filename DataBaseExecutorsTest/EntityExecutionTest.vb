Imports System.Text
Imports Microsoft.VisualStudio.TestTools.UnitTesting
Imports DataBaseExecutors
Imports DataBaseExecutors.Entity

<TestClass()>
Public Class EntityExecutionText

    Private Const ConnectionName As String = "DefaultConnection"

    <ClassInitialize()>
    Public Shared Sub etUp(context As TestContext)
        TestData.Create(ConnectionName)
    End Sub

    <ClassCleanup()>
    Public Shared Sub TearDown()
        TestData.Drop(ConnectionName)
    End Sub

    <TestMethod()>
    Public Sub ReadEntity()

        Dim db As New DBExecution(ConnectionName)
        Dim list As List(Of SalesOrder) = DBEntity.Read(Of SalesOrder)(ConnectionName)
        list = list.OrderBy(Function(x) x.OrderNo).ThenBy(Function(x) x.OrderDetail).ToList
        Dim master As List(Of SalesOrder) = TestData.GetTestData

        Assert.AreEqual(db.sqlReadScalar(Of Integer)("SELECT COUNT(*) FROM " + TestData.TableName), list.Count)

        For i As Integer = 0 To list.Count - 1
            Assert.AreEqual(master(i), list(i))
            Console.WriteLine(list(i))
        Next

    End Sub

    <TestMethod()>
    Public Sub ExecuteEntity()

        Dim db As New DBExecution(ConnectionName)
        Dim order As New SalesOrder
        order.OrderNo = 10
        order.OrderDetail = 100
        order.MaterialCode = "Grape"
        order.Quantity = 12.1
        order.OrderDate = New DateTime(2012, 1, 1)
        order.DeliverDate = DateTime.Now

        'Insert
        Dim exe As KeyValuePair(Of String, DBExecution) = DBEntity.createExecute(order, ConnectionName, False)

        Console.WriteLine(exe.Key)
        Assert.IsTrue(exe.Key.Contains("INSERT"))
        Assert.IsTrue(order.Save(ConnectionName))

        Dim inserted As SalesOrder = order.ReadByKey(Of SalesOrder)(ConnectionName)
        Console.WriteLine("insert:" + order.ToString)
        Console.WriteLine("result:" + inserted.ToString)
        Assert.IsTrue(order.Equals(inserted))

        'Update
        order.MaterialCode = "GrapeFruit"
        order.Quantity = 20
        order.OrderDate = New DateTime(2012, 1, 2)
        order.DeliverDate = DateTime.Now

        exe = DBEntity.createExecute(order, ConnectionName, False)
        Console.WriteLine(exe.Key)
        Assert.IsTrue(exe.Key.Contains("UPDATE"))
        Assert.IsTrue(order.Save(ConnectionName))

        Dim updated As SalesOrder = order.ReadByKey(Of SalesOrder)(ConnectionName)
        Console.WriteLine("update:" + order.ToString)
        Console.WriteLine("result:" + updated.ToString)
        Assert.IsTrue(order.Equals(updated))

        'Delete
        Assert.IsTrue(order.Delete(ConnectionName))
        Assert.IsTrue(order.ReadByKey(Of SalesOrder)(ConnectionName) Is Nothing)

    End Sub


End Class
