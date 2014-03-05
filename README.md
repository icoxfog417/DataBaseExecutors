DataBaseExecutors
=============

DataBaseExecutors is simple database access tool for .NET.  
You don't need to use DataReader or DataAdapter any longer . Just write SQL.  

[API Documents is Here](http://icoxfog417.github.io/DataBaseExecutors/Index.html)  
[Download from Nuget](https://www.nuget.org/packages/DataBaseExecutors/)

Officialy supports Oracle and SqlServer.

Related Repositories

* [ASP.NET DbPerformanceChecker](https://github.com/icoxfog417/ASPNETDbPerformanceChecker)

# How to use
Select
```
Dim db As New DBExecution(ConnectionName)
Dim table As DataTable = db.sqlRead("SELECT * FROM tab")
```

Select Scalar
```
Dim db As New DBExecution(ConnectionName)
Dim count As Integer = db.sqlReadScalar(Of Integer)("SELECT COUNT(*) FROM tab")
```

Functional Recordset processing

* Make xml of users (&lt;users&gt;&lt;user&gt;Mike&lt;/user&gt; ... &lt;/users&gt;)
```
Dim db As New DBExecution(ConnectionName)
Dim xml As String = db.sqlReadToString("SELECT * FROM tab ",Address Of Me.fetch) 'apply function to each record

Private Function fetch(ByVal reader As DbDataReader, ByVal counter As Long) As String
  Return "<user>" + reader.GetStringOrDefault("NAME") + "</user>"
End Function
```

* Make list of user
```
Dim db As New DBExecution(ConnectionName)
Dim users As List(Of User) = db.sqlRead(Of User)("SELECT * FROM tab ",Address Of Me.createUser)

Private Function createUser(ByVal reader As DbDataReader, ByVal counter As Long) As User
  Return New User(reader.GetIntegerOrDefault("ID"),reader.GetStringOrDefault("NAME"))
End Function
```

Select 1 row as dictionary of column name and value
```
Dim db As New DBExecution(ConnectionName)
Dim user As Dictionary(Of String,String) = db.sqlReadOneRow("SELECT * FROM tab WHERE ID = 1")
```

Execute Query(UPDATE/INSERT)
```
Dim db As New DBExecution(ConnectionName)
db.addFilter("pid", 10)
db.addFilter("pName", "Mike")
db.sqlExecution("UPDATE tab SET NAME = :pName WHERE ID = :pId")
```

Execute by DataTable
```
Dim db As New DBExecution(ConnectionName)
db.importTable(table) 'insert/update rows by passing DataTable
db.createTable(table) 'create table from DataTable(you can drop table before create by option).
```

Call database function
```
Dim db As New DBExecution(ConnectionName)
Dim userId As Integer = 10
Dim result As String = db.executeDBFunction(Of String)("GET_USER_NAME",userId)
```

Entity Query
```
Dim users As List(Of User) = DBEntity.Read(Of User)("SELECT * FROM tab",ConnectionName)

Dim db As New DBExecution(ConnectionName)
db.addFilter("pId", 10)
Dim u As User = DBEntity.Read(Of User)("SELECT * FROM tab WHERE UserId = @pId",db).FirstOrDefault
```

Entity Execution
```
Dim user As New User(10,"Mike")
user.Save(ConnectionName)
user.Delete(ConnectionName)
```

About detail , Please see wiki .
