
Namespace DataBaseExecutors

    ''' <summary>
    ''' Row information for using save
    ''' </summary>
    ''' <remarks></remarks>
    Public Class RowInfo

        ''' <summary>
        ''' Delegate to validate or convert value
        ''' </summary>
        ''' <param name="rowInfo"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Delegate Function validateRowInfo(ByVal rowInfo As RowInfo) As RowInfo

        Public Property TableName As String = ""
        Public Property Index As Integer = 0
        Public Property IsValid As Boolean = True

        Private _columnProperties As New List(Of ColumnProperty)
        Public ReadOnly Property ColumnProperties As List(Of ColumnProperty)
            Get
                Return _columnProperties
            End Get
        End Property
        Public Property RowValues As New Dictionary(Of String, Object)
        Public Property Messages As New List(Of String)

        ''' <summary>
        ''' Constructor is private
        ''' </summary>
        ''' <remarks></remarks>
        Private Sub New()
        End Sub

        ''' <summary>
        ''' Copy Constructor
        ''' </summary>
        ''' <remarks></remarks>
        Public Sub New(ByVal rowInfo As RowInfo)
            Me.TableName = rowInfo.TableName
            Me.Index = rowInfo.Index
            Me.IsValid = rowInfo.IsValid
            Me._columnProperties = New List(Of ColumnProperty)(rowInfo._columnProperties)
            Me.RowValues = New Dictionary(Of String, Object)(rowInfo.RowValues)
            Me.Messages = New List(Of String)(rowInfo.Messages)
        End Sub

        ''' <summary>
        ''' Read each row in table
        ''' </summary>
        ''' <param name="table"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Shared Function Read(ByVal table As DataTable) As List(Of RowInfo)
            Dim colProps As List(Of ColumnProperty) = ColumnProperty.Read(table)
            Dim rows As New List(Of RowInfo)

            Dim count As Integer = 0
            For Each row As DataRow In table.Rows
                Dim ri As New RowInfo()
                ri.TableName = table.TableName
                ri.Index = count
                ri._columnProperties = colProps

                For Each c As ColumnProperty In colProps
                    Dim value As Object = row(c.Name)
                    If c.TimeStampFormat IsNot Nothing Then 'If column is timestamp 
                        If c.TimeStampFormat = String.Empty Then
                            value = DateTime.Now
                        Else
                            value = DateTime.Now.ToString(c.TimeStampFormat)
                        End If
                    End If

                    ri.RowValues.Add(c.Name, value)
                Next

                rows.Add(ri)
                count += 1
            Next

            Return rows

        End Function

        ''' <summary>Insert RowInfo</summary>
        ''' <param name="conName"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function Insert(ByVal conName As String) As Boolean
            Dim db As New DBExecution(conName)
            Return Insert(db)
        End Function

        Public Function Insert(ByVal db As DBExecution) As Boolean
            If Not Me.IsValid Then Return True 'exit function
            If Not isExecuteValid() Then Return False

            Dim sql As String = ""
            Dim targets As New List(Of String)
            Dim params As New Dictionary(Of String, Object)

            db.clearFilter()

            For Each param As KeyValuePair(Of String, Object) In RowValues
                Dim cp As ColumnProperty = ColumnProperties.Where(Function(p) p.Name = param.Key).FirstOrDefault
                If cp IsNot Nothing Then
                    If Not cp.IsGenerated And Not cp.IsIgnore Then
                        targets.Add(cp.Name)
                        params.Add(":p" + cp.Name, param.Value)
                    End If
                End If
            Next

            db.addFilter(params)

            sql = "INSERT INTO " + TableName + "( " + String.Join(",", targets) + " ) VALUES ( " + String.Join(",", params.Keys) + " )"

            Dim result As Boolean = db.sqlExecution(sql)
            If Not String.IsNullOrEmpty(db.getErrorMsg) Then
                Messages.Add(db.getErrorMsg)
            End If

            Return result

        End Function

        ''' <summary>Update RowInfo</summary>
        ''' <param name="conName"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function Update(ByVal conName As String) As Boolean
            Dim db As New DBExecution(conName)
            Return Update(db)

        End Function

        Public Function Update(ByVal db As DBExecution) As Boolean

            If Not Me.IsValid Then Return True 'exit function
            If Not isExecuteValid() Then Return False

            Dim sql As String = ""
            Dim targets As New Dictionary(Of String, String)
            Dim params As New Dictionary(Of String, Object)

            db.clearFilter()

            For Each param As KeyValuePair(Of String, Object) In RowValues
                Dim cp As ColumnProperty = ColumnProperties.Where(Function(p) p.Name = param.Key).FirstOrDefault
                If cp IsNot Nothing Then
                    If Not cp.IsKey Then
                        targets.Add(cp.Name, cp.Name + " = :p" + cp.Name)
                        params.Add(":p" + cp.Name, param.Value)
                    End If
                End If
            Next

            db.addFilter(params)
            Dim keyWhere As String = setKeyFilter(db)
            sql = "UPDATE " + TableName + " SET " + String.Join(",", targets.Values) + " WHERE " + keyWhere

            Dim result As Boolean = db.sqlExecution(sql)
            If Not String.IsNullOrEmpty(db.getErrorMsg) Then
                Messages.Add(db.getErrorMsg)
            End If

            Return result

        End Function

        ''' <summary>
        ''' Save RowInfo<br/>
        ''' Before execute, check the record exists or not .
        ''' </summary>
        ''' <param name="conName"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function Save(ByVal conName As String) As Boolean

            Dim db As New DBExecution(conName)
            Return Save(db)

        End Function

        Public Function Save(ByVal db As DBExecution) As Boolean

            db.clearFilter()
            Dim rowCount As Integer = db.sqlReadScalar(Of Integer)("SELECT COUNT(*) FROM " + TableName + " WHERE " + setKeyFilter(db))
            If rowCount > 1 Then
                Messages.Add("Primary Key that set to DataTable is not unique.")
                Return False
            Else

                If rowCount = 0 Then
                    Return Insert(db)
                Else
                    Return Update(db)
                End If

            End If

        End Function

        Private Function isExecuteValid() As Boolean
            Dim result As Boolean = True
            If String.IsNullOrEmpty(TableName) Then Messages.Add("Table name is not set")
            If ColumnProperties.Where(Function(p) p.IsKey).Count = 0 Then Messages.Add("Primary Key is not set")

            Return Messages.Count = 0

        End Function

        Private Function setKeyFilter(ByRef db As DBExecution) As String

            Dim order As Integer = 1
            Dim filters As New List(Of String)

            For Each cp As ColumnProperty In ColumnProperties.Where(Function(p) p.IsKey)
                Dim pName As String = ":pKey" + order.ToString
                db.addFilter(pName, RowValues(cp.Name))
                filters.Add(cp.Name + " = " + pName)
                order += 1
            Next

            Dim where As String = String.Join(" AND ", filters)
            Return where

        End Function

    End Class


End Namespace
