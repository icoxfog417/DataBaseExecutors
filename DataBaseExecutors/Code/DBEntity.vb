Imports System.Reflection
Imports System.Data.Common

Namespace DataBaseExecutors

    Public Class DBEntity

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

        Public Shared Function Read(Of T As IDBEntity)(ByVal conName As String) As List(Of T)
            Dim db As New DBExecution(conName)
            Dim sqlFormat As String = "SELECT * FROM {0}"

            Dim table As String = GetSource(GetType(T))
            If not String.IsNullOrEmpty(table) Then
                Return Read(Of T)(String.Format(sqlFormat, table), db)
            Else
                Return New List(Of T) 'チェインして使用することを想定し、Nothingは返さない(空インスタンスを返却)
            End If

        End Function

        Public Shared Function Read(Of T As IDBEntity)(ByVal sql As String, ByVal conName As String) As List(Of T)
            Dim db As New DBExecution(conName)
            Return Read(Of T)(sql, db)
        End Function

        Public Shared Function Read(Of T As IDBEntity)(ByVal sql As String, ByVal db As DBExecution) As List(Of T)
            Dim result As List(Of T) = db.sqlRead(Of T)(sql, AddressOf createInstance(Of T))
            If Not String.IsNullOrEmpty(db.getErrorMsg) Then
                Throw New Exception(db.getErrorMsg)
            End If

            Return result
        End Function

        Private Shared Function createInstance(Of T As IDBEntity)(ByVal reader As DbDataReader, ByVal counter As Long) As T
            Dim model As T = Activator.CreateInstance(Of T)()
            Dim props As List(Of DBEntityProperty) = GetDBProperties(model, False) 'DBColumnが設定されているか否かによらず、一致するプロパティに値をセット

            '列定義の読込
            Dim columnList As List(Of String) = (From i As Integer In Enumerable.Range(0, reader.FieldCount)
                                                Select reader.GetName(i)).ToList

            For Each col As String In columnList

                Dim prop As DBEntityProperty = (From p As DBEntityProperty In props Where p.ColumnName = col Select p).FirstOrDefault
                If prop IsNot Nothing Then
                    Dim value As Object = reader.GetStringOrDefault(prop.ColumnName)

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
                            End If
                            value = d
                    End Select

                    If prop.Property.GetSetMethod() IsNot Nothing Then 'Setterが存在する場合、値をセット
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

        Public Shared Function createExecute(ByVal obj As IDBEntity, ByVal conName As String, ByVal isSave As Boolean) As KeyValuePair(Of String, DBExecution)
            'オブジェクトのチェック
            If String.IsNullOrEmpty(conName) Then Throw New Exception("ConnectionName is Missing")
            Dim db As New DBExecution(conName)
            Return createExecute(obj, db, isSave)

        End Function

        Public Shared Function createExecute(ByVal obj As IDBEntity, ByVal db As DBExecution, ByVal isSave As Boolean) As KeyValuePair(Of String, DBExecution)

            Dim table As String = GetSource(obj.GetType)
            Dim props As List(Of DBEntityProperty) = GetDBProperties(obj)

            If String.IsNullOrEmpty(table) OrElse props.Count = 0 Then
                Throw New Exception("The Table Or Properties for INSERT/UPDATE don't Exist.")
            ElseIf props.Where(Function(x) x.Column.IsKey = True).Count = 0 Then
                Throw New Exception("There is no key")
            End If

            '存在チェック or DELETE 用SQL
            Dim where As New List(Of String)
            For Each key As DBEntityProperty In props.Where(Function(x) x.Column.IsKey = True)
                db.addFilter(key.ParameterName, key.Value)
                where.Add(key.ColumnName + " = :" + key.ParameterName)
            Next

            Dim cnt As Integer = 0
            If isSave Then
                cnt = db.sqlReadScalar(Of Integer)("SELECT COUNT(*) FROM " + table + " WHERE " + String.Join(" AND ", where))
            Else
                '！削除の場合ここでReturnし後続の処理は行わない
                Return New KeyValuePair(Of String, DBExecution)("DELETE FROM " + table + " WHERE " + String.Join(" AND ", where), db)
            End If

            '実行SQL作成
            Dim sql As String = ""
            where.Clear()
            If cnt > 1 Then 'UpDate
                Throw New Exception("指定されたキーでレコードが一意に特定できません")
            ElseIf cnt = 1 Then
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

            Return New KeyValuePair(Of String, DBExecution)(sql, db)

        End Function

        Public Shared Function GetDBProperties(ByVal obj As IDBEntity, Optional ByVal isOnlyDBColumn As Boolean = True) As List(Of DBEntityProperty)
            Dim result As New List(Of DBEntityProperty)

            Dim props As PropertyInfo() = obj.GetType.GetProperties
            Dim propQuery = From p As PropertyInfo In props
                            Let attr As DBColumnAttribute = p.GetCustomAttributes(GetType(DBColumnAttribute), True).SingleOrDefault
                            Order By If(attr IsNot Nothing AndAlso attr.IsKey, 1, 0), If(attr IsNot Nothing, attr.Order.ToString, "z"), p.Name
                            Select p, attr

            For Each prop In propQuery
                If Not (isOnlyDBColumn AndAlso prop.attr Is Nothing) Then
                    If prop.p.GetGetMethod() IsNot Nothing Then
                        Try
                            result.Add(New DBEntityProperty(prop.attr, prop.p, convertValue(prop.p.GetValue(obj, Nothing), prop.attr)))
                        Catch ex As Exception
                            Continue For
                        End Try
                    End If
                End If
            Next

            Return result

        End Function


        Private Shared Function convertValue(value As Object, attr As DBColumnAttribute) As Object
            Dim result As Object = value
            If attr IsNot Nothing AndAlso TypeOf value Is DateTime Then
                If CType(value, DateTime) = DateTime.MinValue Then
                    result = String.Empty '最小値の場合Nullを設定
                Else
                    If Not String.IsNullOrEmpty(attr.Format) Then
                        result = CType(value, DateTime).ToString(attr.Format)
                    End If
                End If
            End If
            If TypeOf value Is Boolean Then result = If(value = True, "1", "0")
            Return result
        End Function


    End Class

    'DBEntityExecution対象クラスを示すマーカーインタフェース
    Public Interface IDBEntity
    End Interface

    Public Class DBColumnAttribute
        Inherits System.Attribute
        Public Property Name As String
        Public Property IsKey As Boolean = False
        Public Property Order As Integer = 0
        Public Property Format As String = ""
        Public Property IsDbGenerate As Boolean = False
    End Class

    Public Class DBSourceAttribute
        Inherits System.Attribute
        Public Property Table As String
        Public Property View As String
    End Class

    Public Class DBEntityProperty

        Private _column As DBColumnAttribute = Nothing
        Public ReadOnly Property Column As DBColumnAttribute
            Get
                Return _column
            End Get
        End Property

        Private _property As PropertyInfo = Nothing
        Public ReadOnly Property [Property] As PropertyInfo
            Get
                Return _property
            End Get
        End Property

        Private _value As Object = Nothing
        Public ReadOnly Property Value As Object
            Get
                Return _Value
            End Get
        End Property

        Public ReadOnly Property Name As String
            Get
                Return _property.Name
            End Get
        End Property
        Public ReadOnly Property Type As Type
            Get
                Return _property.PropertyType
            End Get
        End Property

        Public ReadOnly Property ColumnName() As String
            Get
                If Column IsNot Nothing AndAlso Not String.IsNullOrEmpty(Column.Name) Then
                    Return Column.Name
                Else
                    Return Name
                End If
            End Get
        End Property

        Public ReadOnly Property ParameterName() As String
            Get
                Return "pm" + ColumnName
            End Get
        End Property

        Public Sub New(ByVal column As DBColumnAttribute, ByVal prop As PropertyInfo, ByVal value As Object)
            Me._column = column
            Me._property = prop
            Me._value = value
        End Sub

    End Class


End Namespace
