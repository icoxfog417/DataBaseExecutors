Imports System.Text
Imports Microsoft.VisualStudio.TestTools.UnitTesting
Imports DataBaseExecutors

<TestClass()>
Public Class EntityExecutionText

    Private Shared connectionName As String = "OracleConnect" '"SqlSConnect"
    Private _testDataCount As Integer = 0

    <ClassInitialize()>
    Public Shared Sub setUp(context As TestContext)
        Dim db = New DBExecution(connectionName)

        'テーブル削除(事前に)
        db.sqlExecution("DROP TABLE ORDER_DATA")
        'テーブル作成
        Select Case connectionName
            Case "OracleConnect"
                db.sqlExecution("CREATE TABLE ORDER_DATA ( NO NUMBER(10,0) NOT NULL, DETAIL_NO NUMBER(5,0) NOT NULL,HIN_CD VARCHAR2(20), AMOUNT NUMBER(10,3),ORDER_DATE VARCHAR2(8))")
            Case Else
                db.sqlExecution("CREATE TABLE ORDER_DATA ( NO NUMERIC(10,0) NOT NULL,DETAIL_NO NUMERIC(5,0) NOT NULL,HIN_CD VARCHAR(20), AMOUNT NUMERIC(10,3),ORDER_DATE VARCHAR2(8))")
        End Select

    End Sub

    <TestInitialize()>
    Public Sub beforeTest()
        Dim db = New DBExecution(connectionName)

        'データ投入
        db.sqlExecution("INSERT INTO ORDER_DATA VALUES( 1, 100,'POKKY',120.3,'20131101' )")
        db.sqlExecution("INSERT INTO ORDER_DATA VALUES( 2, 100,'POLINKY',80,'20131102' )")
        db.sqlExecution("INSERT INTO ORDER_DATA VALUES( 3, 100,'TOPPO',110.5,'20131104' )")
        _testDataCount = 3

    End Sub

    <TestCleanup()>
    Public Sub afterTest()
        Dim db = New DBExecution(connectionName)

        '投入データ削除
        db.sqlExecution("DELETE FROM ORDER_DATA")

    End Sub

    <TestMethod()>
    Public Sub ReadEntity()
        Dim sql As String = "SELECT * FROM ORDER_DATA"
        Dim list As List(Of ORDER_DATA) = DBEntity.Read(Of ORDER_DATA)(sql, connectionName)

        Assert.AreEqual(_testDataCount, list.Count)

        For Each item As ORDER_DATA In list
            Console.WriteLine(item)
        Next

    End Sub

    <TestMethod()>
    Public Sub UpdateEntity()
        Dim order As New ORDER_DATA
        order.NO = 1
        order.DETAIL_NO = 100
        order.HIN_CD = "APOLO"
        order.KIN = 120
        order.ORDER_DATE = DateTime.Now

        Dim exe As KeyValuePair(Of String, DBExecution) = DBEntity.createExecute(order, connectionName, True)

        Console.WriteLine(exe.Key)
        Assert.IsTrue(exe.Key.Contains("UPDATE"))

        Assert.IsTrue(exe.Value.sqlExecution(exe.Key))

    End Sub

    <TestMethod()>
    Public Sub InsertEntity()
        Dim order As New ORDER_DATA()
        order.NO = 1
        order.DETAIL_NO = 200
        order.HIN_CD = "NEW ITEM"
        order.KIN = 900
        order.ORDER_DATE = DateTime.Now

        Dim exe As KeyValuePair(Of String, DBExecution) = DBEntity.createExecute(order, connectionName, True)

        Console.WriteLine(exe.Key)
        Assert.IsTrue(exe.Key.Contains("INSERT"))

        Assert.IsTrue(order.Save(connectionName))

        Assert.IsTrue(order.Delete(connectionName)) '削除しておく

    End Sub

End Class

<DBSource(Table:="ORDER_DATA")>
Public Class ORDER_DATA
    Implements IDBEntity

    <DBColumn(IsKey:=True, Order:=1)>
    Public Property NO As Integer = 0
    <DBColumn(IsKey:=True, Order:=2)>
    Public Property DETAIL_NO As Integer = 0

    <DBColumn()>
    Public Property HIN_CD As String = ""
    <DBColumn(Name:="AMOUNT")>
    Public Property KIN As Decimal = 0

    <DBColumn(Format:="yyyyMMdd")>
    Public Property ORDER_DATE As DateTime

    Public Overrides Function ToString() As String
        Dim result = NO.ToString + "/" + DETAIL_NO.ToString + ":" + HIN_CD + " \" + KIN.ToString + " @" + ORDER_DATE.ToString("yyyyMMdd")
        Return result
    End Function

End Class