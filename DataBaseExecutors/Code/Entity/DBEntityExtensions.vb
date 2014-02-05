Imports System.Runtime.CompilerServices
Imports System.Data.Common
Imports System.Reflection
Imports DataBaseExecutors

Namespace DataBaseExecutors.Entity

    ''' <summary>
    ''' Extension for the class that implements IDBEntiry.<br/>
    ''' This Extension enables the class to Save/Delete themselves.
    ''' </summary>
    ''' <remarks></remarks>
    Public Module DBEntityExtension

        ''' <summary>
        ''' Get tavle/view name of class
        ''' </summary>
        <Extension()> _
        Public Function Source(ByVal obj As IDBEntity) As DBSourceAttribute
            Dim dbTable As DBSourceAttribute = obj.GetType.GetCustomAttributes(GetType(DBSourceAttribute), True).FirstOrDefault
            Return dbTable
        End Function

        ''' <summary>
        ''' Save to database.<br/>
        ''' The main process is delegated to DBEntity class.
        ''' </summary>
        ''' <param name="obj"></param>
        ''' <param name="conName">connection name for database</param>
        <Extension()> _
        Public Function Save(ByVal obj As IDBEntity, ByVal conName As String) As Boolean
            Dim exe As KeyValuePair(Of String, DBExecution) = DBEntity.createExecute(obj, conName, False)
            Dim result As Boolean = exe.Value.sqlExecution(exe.Key)
            If Not result Then
                Throw New Exception(exe.Value.getErrorMsg)
            Else
                Return True
            End If
        End Function


        ''' <summary>
        ''' Save to database by using passed DBExecution.<br/>
        ''' Mainly, it's for transaction process (open transaction before it,then execute save).
        ''' </summary>
        ''' <param name="obj"></param>
        ''' <param name="db"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        <Extension()> _
        Public Function Save(ByVal obj As IDBEntity, ByVal db As DBExecution) As Boolean
            Dim exe As KeyValuePair(Of String, DBExecution) = DBEntity.createExecute(obj, db, False)
            Dim result As Boolean = exe.Value.sqlExecution(exe.Key)
            If Not result Then
                Throw New Exception(exe.Value.getErrorMsg)
            Else
                Return True
            End If
        End Function

        ''' <summary>
        ''' Delete from database.<br/>
        ''' Delete is executed by key property of object (key is set by DBColumn attribute).
        ''' </summary>
        ''' <param name="obj"></param>
        ''' <param name="conName"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        <Extension()> _
        Public Function Delete(ByVal obj As IDBEntity, ByVal conName As String) As Boolean
            Dim exe As KeyValuePair(Of String, DBExecution) = DBEntity.createExecute(obj, conName, True)
            Dim result As Boolean = exe.Value.sqlExecution(exe.Key)
            If Not result Then
                Throw New Exception(exe.Value.getErrorMsg)
            Else
                Return True
            End If
        End Function

        ''' <summary>
        ''' Get same key record from database.
        ''' </summary>
        ''' <typeparam name="T"></typeparam>
        ''' <param name="obj"></param>
        ''' <param name="conName"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
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

                If props.Count = 0 Then Throw New Exception("No Key is defined in this DBEntity. Please set it by DBColumn attribute.")
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
