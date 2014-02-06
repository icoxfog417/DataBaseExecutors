DataBaseExecutors
=============

DataBaseExecutors is simple database access tool for .NET.  
You don't need to use DataReader or DataAdapter any longer . Just write SQL.  

[API Documents is Here](http://icoxfog417.github.io/DataBaseExecutors/Index.html)  
[Download from Nuget](https://www.nuget.org/packages/DataBaseExecutors/)

Officialy supports Oracle and SqlServer.

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

Execute
```
Dim db As New DBExecution(ConnectionName)
db.addFilter("pid", 10)
db.addFilter("pName", "Mike")
db.sqlExecution("UPDATE tab SET NAME = :pName WHERE id = :pId")
```

Call database function
```
Dim db As New DBExecution(ConnectionName)
Dim userId As Integer = 10
Dim result As String = db.executeDBFunction(Of String)("GET_USER_NAME",userId)
```

Entity Query
```
Dim user As New User(10,"Mike")
user.Save(ConnectionName)
user.Delete(ConnectionName)

Dim users As List(Of User) = DBEntity.Read(Of User)("SELECT * FROM USER_TABLE")
```

About detail , Please see wiki .
