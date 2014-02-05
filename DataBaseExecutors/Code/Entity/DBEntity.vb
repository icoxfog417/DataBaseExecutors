Imports System.Reflection
Imports System.Data.Common
Imports DataBaseExecutors

Namespace DataBaseExecutors.Entity

    ''' <summary>
    ''' Marker interface for DBEntityExecution
    ''' </summary>
    ''' <remarks></remarks>
    Public Interface IDBEntity
    End Interface

    ''' <summary>
    ''' DBExecution wrapper to deal with sql execution from object's method
    ''' </summary>
    ''' <remarks></remarks>
    Public Class DBEntity

        ''' <summary>
        ''' Get table or view of class.<br/>
        ''' You can set it by DBSource attribute.
        ''' </summary>
        ''' <param name="t">type of class</param>
        ''' <param name="isForSelect">
        ''' For select or not. Because you can set not only table but also view for selecting in DBSource attribute.
        ''' </param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Shared Function GetSource(ByVal t As Type, Optional ByVal isForSelect As Boolean = True) As String
            Dim source As String = ""
            Dim dbTable As DBSourceAttribute = t.GetCustomAttributes(GetType(DBSourceAttribute), True).FirstOrDefault
            If dbTable IsNot Nothing Then
                If isForSelect And dbTable.View <> "" Then
                    source = dbTable.View
                Else
                    source = dbTable.Table
                End If
            End If

            Return source

        End Function

        ''' <summary>
        ''' Read from table/view of class.
        ''' </summary>
        ''' <typeparam name="T"></typeparam>
        ''' <param name="conName"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Shared Function Read(Of T As IDBEntity)(ByVal conName As String) As List(Of T)
            Dim db As New DBExecution(conName)
            Dim sqlFormat As String = "SELECT * FROM {0}"

            Dim table As String = GetSource(GetType(T))
            If Not String.IsNullOrEmpty(table) Then
                Return Read(Of T)(String.Format(sqlFormat, table), db)
            Else
                Return New List(Of T) 'Don't return Nothing (for method chain)
            End If

        End Function

        ''' <summary>
        ''' Read from sql.
        ''' </summary>
        ''' <typeparam name="T"></typeparam>
        ''' <param name="sql"></param>
        ''' <param name="conName"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Shared Function Read(Of T As IDBEntity)(ByVal sql As String, ByVal conName As String) As List(Of T)
            Dim db As New DBExecution(conName)
            Return Read(Of T)(sql, db)
        End Function

        ''' <summary>
        ''' Read from parameter query.parameter is passed by DBExecution
        ''' </summary>
        ''' <typeparam name="T"></typeparam>
        ''' <param name="sql"></param>
        ''' <param name="db"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Shared Function Read(Of T As IDBEntity)(ByVal sql As String, ByVal db As DBExecution) As List(Of T)
            Dim result As List(Of T) = db.sqlRead(Of T)(sql, AddressOf createInstance(Of T))
            If Not String.IsNullOrEmpty(db.getErrorMsg) Then
                Throw New Exception(db.getErrorMsg)
            End If

            Return result
        End Function

        ''' <summary>
        ''' Convert DbDataReader to Object.This works as delegate function of DBExecution.sqlRead.<br/>
        ''' In general, value is set to property from same name column's value.<br/>
        ''' You can make some rule in DBColumn attribute.
        ''' </summary>
        ''' <typeparam name="T"></typeparam>
        ''' <param name="reader"></param>
        ''' <param name="counter"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Private Shared Function createInstance(Of T As IDBEntity)(ByVal reader As DbDataReader, ByVal counter As Long) As T
            Dim model As T = Activator.CreateInstance(Of T)()

            'Extract properties from class. 
            'The process doesn't consider that DBColumn attribute is set to property when setting the value to property.
            Dim props As List(Of DBEntityProperty) = GetDBProperties(model, False)

            'read column definition
            Dim columnList As List(Of String) = (From i As Integer In Enumerable.Range(0, reader.FieldCount)
                                                Select reader.GetName(i)).ToList

            For Each col As String In columnList

                'Not case-sensitive between the column name and property name(because Oracle make column name to upper).
                Dim prop As DBEntityProperty = (From p As DBEntityProperty In props Where p.ColumnName.ToUpper = col.ToUpper Select p).FirstOrDefault
                If prop IsNot Nothing Then
                    Dim value As Object = reader.GetStringOrDefault(prop.ColumnName)

                    'TODO Sometimes it needs db peculiarity process(use adapter).
                    Select Case prop.Type
                        Case GetType(Decimal)
                            value = reader.GetDecimalOrDefault(prop.ColumnName)
                        Case GetType(Integer)
                            value = reader.GetIntegerOrDefault(prop.ColumnName)
                        Case GetType(Double)
                            value = If(String.IsNullOrEmpty(value), 0, Double.Parse(value))
                        Case GetType(Boolean)
                            value = If(value.Equals("1"), True, False)
                        Case GetType(DateTime)
                            Dim d As DateTime = DateTime.MinValue
                            If prop.Column IsNot Nothing AndAlso DateTime.TryParseExact(value.ToString, prop.Column.Format, _
                                                                                 System.Globalization.DateTimeFormatInfo.InvariantInfo, _
                                                                                 System.Globalization.DateTimeStyles.None, d) Then
                            ElseIf DateTime.TryParse(value, d) Then 'Default parse

                            End If
                            value = d
                    End Select

                    If prop.Property.GetSetMethod() IsNot Nothing Then 'Confirm existence of setter
                        Try
                            prop.Property.SetValue(model, value, Nothing)
                        Catch ex As Exception
                            Continue For
                        End Try
                    End If
                End If

            Next

            Return model

        End Function

        ''' <summary>
        ''' Create DBExecution to execute query.
        ''' </summary>
        ''' <param name="obj"></param>
        ''' <param name="conName"></param>
        ''' <param name="isDelete">true:deal with Delete false:deal with Save</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Shared Function createExecute(ByVal obj As IDBEntity, ByVal conName As String, ByVal isDelete As Boolean) As KeyValuePair(Of String, DBExecution)

            If String.IsNullOrEmpty(conName) Then Throw New Exception("ConnectionName is Missing")
            Dim db As New DBExecution(conName)
            Return createExecute(obj, db, isDelete)

        End Function

        ''' <summary>
        ''' Create DBExecution to execute query with parameters.<br/>
        ''' Parameters is passed by DBExecution.
        ''' </summary>
        ''' <param name="obj"></param>
        ''' <param name="db"></param>
        ''' <param name="isDelete"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Shared Function createExecute(ByVal obj As IDBEntity, ByVal db As DBExecution, ByVal isDelete As Boolean) As KeyValuePair(Of String, DBExecution)

            Dim table As String = GetSource(obj.GetType)
            Dim props As List(Of DBEntityProperty) = GetDBProperties(obj)

            If String.IsNullOrEmpty(table) OrElse props.Count = 0 Then
                Throw New Exception("The Table Or Properties for INSERT/UPDATE don't Exist.")
            ElseIf props.Where(Function(x) x.Column.IsKey = True).Count = 0 Then
                Throw New Exception("There is no key")
            End If

            'Make parameter and where statement for record confirmation
            Dim where As New List(Of String)
            For Each key As DBEntityProperty In props.Where(Function(x) x.Column.IsKey = True)
                db.addFilter(key.ParameterName, key.Value)
                where.Add(key.ColumnName + " = :" + key.ParameterName)
            Next

            Dim cnt As Integer = 0
            If Not isDelete Then
                'If not isDelete,confirm the existence of record
                cnt = db.sqlReadScalar(Of Integer)("SELECT COUNT(*) FROM " + table + " WHERE " + String.Join(" AND ", where))
            Else
                'If isDelete,execute delete and return(exit funciton).
                Return New KeyValuePair(Of String, DBExecution)("DELETE FROM " + table + " WHERE " + String.Join(" AND ", where), db)
            End If

            'Make sql for Save(INSERT or UPDATE)
            Dim sql As String = ""
            where.Clear()
            If cnt > 1 Then 'Exception: duplicate record exist.
                Throw New Exception("Can't specify one record by key.")

            ElseIf cnt = 1 Then 'Update
                Dim updPart As New List(Of String)
                For Each item As DBEntityProperty In props
                    If Not item.Column.IsDbGenerate Then
                        db.addFilter(item.ParameterName, item.Value)
                        If Not item.Column.IsKey Then
                            updPart.Add(item.ColumnName + " = :" + item.ParameterName)
                        Else
                            where.Add(item.ColumnName + " = :" + item.ParameterName)
                        End If
                    End If
                Next
                sql = "UPDATE " + table + " SET " + String.Join(",", updPart) + " WHERE " + String.Join(" AND ", where)

            Else 'Insert
                Dim tgtPart As New List(Of String)
                Dim insPart As New List(Of String)
                For Each item As DBEntityProperty In props
                    If Not item.Column.IsDbGenerate Then
                        db.addFilter(item.ParameterName, item.Value)
                        tgtPart.Add(item.ColumnName)
                        insPart.Add(":" + item.ParameterName)
                    End If
                Next
                sql = "INSERT INTO " + table + "(" + String.Join(",", tgtPart) + ") VALUES (" + String.Join(",", insPart) + ") "
            End If

            Return New KeyValuePair(Of String, DBExecution)(sql, db) 'Return sql and parameter(DBExecution)

        End Function

        ''' <summary>
        ''' Get list of DBColumn properties.<br/>
        ''' You can also extract properties that don't have DBColumn attribute.
        ''' </summary>
        ''' <param name="obj"></param>
        ''' <param name="isOnlyDBColumn">extract only properties that have DBColumn attribute or not.</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Shared Function GetDBProperties(ByVal obj As IDBEntity, Optional ByVal isOnlyDBColumn As Boolean = True) As List(Of DBEntityProperty)
            Dim result As New List(Of DBEntityProperty)

            Dim props As PropertyInfo() = obj.GetType.GetProperties
            Dim propQuery = From p As PropertyInfo In props
                            Let attr As DBColumnAttribute = p.GetCustomAttributes(GetType(DBColumnAttribute), True).SingleOrDefault
                            Order By If(attr IsNot Nothing AndAlso attr.IsKey, 1, 0), If(attr IsNot Nothing, attr.Order.ToString, "z"), p.Name
                            Select p, attr

            'About "If(attr IsNot Nothing, attr.Order.ToString, "z")". It's for putting off properties that don't have DBColumn's Orcer .

            For Each prop In propQuery
                If Not (isOnlyDBColumn AndAlso prop.attr Is Nothing) Then
                    If prop.p.GetGetMethod() IsNot Nothing Then
                        Try
                            result.Add(New DBEntityProperty(prop.attr, prop.p, convertValue(prop.p.GetValue(obj, Nothing), prop.attr)))
                        Catch ex As Exception
                            Continue For 'if convertValue failed,continue the process
                        End Try
                    End If
                End If
            Next

            Return result

        End Function

        ''' <summary>
        ''' Convert value to database value.
        ''' </summary>
        ''' <param name="value"></param>
        ''' <param name="attr"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Private Shared Function convertValue(value As Object, attr As DBColumnAttribute) As Object
            Dim result As Object = value
            If attr IsNot Nothing AndAlso TypeOf value Is DateTime Then
                If CType(value, DateTime) = DateTime.MinValue Then 'Initial Value of Datetime.
                    result = Nothing 'Null
                Else
                    If Not String.IsNullOrEmpty(attr.Format) Then
                        result = CType(value, DateTime).ToString(attr.Format) 'If format is set, convert to String.
                    End If
                End If
            End If
            If TypeOf value Is Boolean Then result = If(value = True, "1", "0") 'Boolean is changed to 1/0
            Return result
        End Function


    End Class

End Namespace
