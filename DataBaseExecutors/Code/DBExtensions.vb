Imports System.Runtime.CompilerServices
Imports System.Data.Common
Imports System.Reflection

Namespace DataBaseExecutors

    '拡張メソッド/DBexecutionに移行予定
    Public Module ExtendDbDataReader
        <Extension()> _
        Public Function GetDecimalOrDefault(ByVal reader As DbDataReader, ByVal name As String) As Decimal
            If IsDBNull(reader(name)) OrElse String.IsNullOrEmpty(reader(name)) Then
                Return 0
            Else
                Dim d As Decimal = 0
                If Decimal.TryParse(reader(name).ToString, d) Then Return d Else Return 0
            End If
        End Function

        <Extension()> _
        Public Function GetIntegerOrDefault(ByVal reader As DbDataReader, ByVal name As String) As Integer
            If IsDBNull(reader(name)) OrElse String.IsNullOrEmpty(reader(name)) Then
                Return 0
            Else
                Dim i As Integer = 0
                If Integer.TryParse(reader(name).ToString, i) Then Return i Else Return 0
            End If
        End Function

        <Extension()> _
        Public Function GetStringOrDefault(ByVal reader As DbDataReader, ByVal name As String) As String
            If IsDBNull(reader(name)) OrElse String.IsNullOrEmpty(reader(name)) Then
                Return String.Empty
            Else
                Return reader(name).ToString
            End If
        End Function

    End Module

    Public Module DBEntityExtension

        <Extension()> _
        Public Function Source(ByVal obj As IDBEntity) As DBSourceAttribute
            Dim dbTable As DBSourceAttribute = obj.GetType.GetCustomAttributes(GetType(DBSourceAttribute), True).FirstOrDefault
            Return dbTable
        End Function

        <Extension()> _
        Public Function Save(ByVal obj As IDBEntity, ByVal conName As String) As Boolean
            Dim exe As KeyValuePair(Of String, DBExecution) = DBEntity.createExecute(obj, conName, True)
            Dim result As Boolean = exe.Value.sqlExecution(exe.Key)
            If Not result Then
                Throw New Exception(exe.Value.getErrorMsg)
            Else
                Return True
            End If
        End Function


        ''' <summary>
        ''' トランザクション処理用
        ''' ※配列を渡して処理できるようにすべきか、要検討
        ''' </summary>
        ''' <param name="obj"></param>
        ''' <param name="db"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        <Extension()> _
        Public Function Save(ByVal obj As IDBEntity, ByVal db As DBExecution) As Boolean
            Dim exe As KeyValuePair(Of String, DBExecution) = DBEntity.createExecute(obj, db, True)
            Dim result As Boolean = exe.Value.sqlExecution(exe.Key)
            If Not result Then
                Throw New Exception(exe.Value.getErrorMsg)
            Else
                Return True
            End If
        End Function

        <Extension()> _
        Public Function Delete(ByVal obj As IDBEntity, ByVal conName As String) As Boolean
            Dim exe As KeyValuePair(Of String, DBExecution) = DBEntity.createExecute(obj, conName, False)
            Dim result As Boolean = exe.Value.sqlExecution(exe.Key)
            If Not result Then
                Throw New Exception(exe.Value.getErrorMsg)
            Else
                Return True
            End If
        End Function

        <Extension()> _
        Public Function ReadByKey(Of T As IDBEntity)(ByVal obj As IDBEntity, ByVal conName As String) As T

            Dim table As String = DBEntity.GetSource(obj.GetType)

            If String.IsNullOrEmpty(table) Then
                Return Nothing
            Else
                Dim props As List(Of DBEntityProperty) = DBEntity.GetDBProperties(obj).Where(Function(x) x.Column.IsKey).ToList
                Dim db As New DBExecution(conName)

                Dim sql As String = "SELECT * FROM " + table + " WHERE "
                Dim where As New List(Of String)

                If props.Count = 0 Then Throw New Exception("No Key is defined in this DBEntity")
                For i As Integer = 0 To props.Count - 1
                    db.addFilter(props(i).ParameterName, props(i).Value)
                    If i > 0 Then sql += " AND "
                    sql += props(i).ColumnName + " = :" + props(i).ParameterName

                Next

                Return DBEntity.Read(Of T)(sql, db).FirstOrDefault

            End If

        End Function

    End Module

End Namespace
