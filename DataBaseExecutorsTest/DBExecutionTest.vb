Imports DataBaseExecutors
Imports System.Data.Common

<TestClass()>
Public Class DBExecutionTest

    Private Shared connectionName As String = "OracleConnect" '"SqlSConnect"
    Private _testDataCount As Integer = 0

    <ClassInitialize()>
    Public Shared Sub setUp(context As TestContext)
        Dim db = New DBExecution(connectionName)

        'テーブル削除(事前に)
        db.sqlExecution("DROP TABLE USER_DATA")
        'テーブル作成
        Select Case connectionName
            Case "OracleConnect"
                db.sqlExecution("CREATE TABLE USER_DATA ( ID NUMBER(10,0) NOT NULL, NAME VARCHAR2(20) )")
            Case Else
                db.sqlExecution("CREATE TABLE USER_DATA ( ID NUMERIC(10,0) NOT NULL,NAME VARCHAR(20) )")
        End Select

    End Sub

    <TestInitialize()>
    Public Sub beforeTest()
        Dim db = New DBExecution(connectionName)

        'データ投入
        db.sqlExecution("INSERT INTO USER_DATA VALUES( 1, 'TARO' )")
        db.sqlExecution("INSERT INTO USER_DATA VALUES( 2, 'MANA' )")
        db.sqlExecution("INSERT INTO USER_DATA VALUES( 3, 'KOBO' )")
        db.sqlExecution("INSERT INTO USER_DATA VALUES( 4, 'じゃじゃ○' )") 'マルチバイト文字列
        db.sqlExecution("INSERT INTO USER_DATA VALUES( 5, 'ピッコロ' )")

        _testDataCount = 5

    End Sub

    <TestCleanup()>
    Public Sub afterTest()
        Dim db = New DBExecution(connectionName)

        '投入データ削除
        db.sqlExecution("DELETE FROM USER_DATA")

    End Sub

    <TestMethod()>
    Public Sub SqlExecute()
        Dim db = New DBExecution(connectionName)

        'データ更新
        db.addFilter("pid", "2")
        db.sqlExecution("UPDATE USER_DATA SET NAME = 'MANA KANA' WHERE ID = :pid")

        Assert.AreEqual(String.Empty, db.getErrorMsg)
        Assert.AreEqual(1, db.getRowAffected)

    End Sub

    <TestMethod()>
    Public Sub SqlRead()

        Dim db = New DBExecution(connectionName)

        'DataTable取得
        Dim table As DataTable = db.sqlRead("SELECT * FROM USER_DATA ORDER BY ID")
        Assert.AreEqual(_testDataCount, table.Rows.Count)

        For i As Integer = 1 To table.Rows.Count
            Assert.AreEqual(i, CInt(table.Rows(i - 1)(0).ToString))
        Next

        'オブジェクト取得(匿名クラスを使用)
        Dim result As List(Of Object) _
            = db.sqlRead(Of Object)("SELECT * FROM USER_DATA ORDER BY ID", _
                Function(reader As DbDataReader, counter As Long) As Object
                    Return New With {.ID = reader(0), .NAME = reader(1)}
                End Function
              )

        For i As Integer = 0 To table.Rows.Count - 1
            Assert.AreEqual(table.Rows(i)(0), result(i).ID)
            Assert.AreEqual(table.Rows(i)(1), result(i).NAME)
        Next


    End Sub


    <TestMethod()>
    Public Sub SqlReadOthers()

        Dim db = New DBExecution(connectionName)

        'スカラー値取得
        Dim cnt As Integer = db.sqlReadScalar(Of Integer)("SELECT COUNT(*) AS CNT FROM USER_DATA")
        Assert.AreEqual(_testDataCount, cnt)

        '行取得
        db.addFilter("pid", "2")
        Dim result As Dictionary(Of String, String) = db.sqlReadOneRow("SELECT * FROM USER_DATA WHERE ID = :pid ")

        Assert.AreEqual(2, result.Count)
        Assert.AreEqual("MANA", result("NAME"))

    End Sub

End Class
