Imports Microsoft.VisualBasic
Imports System.Data

Namespace DataBaseExecutors.Aggregation

    ''' <summary>
    ''' The class for aggregating DataTable.You can get List of class with subtotal items.<br/>
    ''' Caution:You have to sort DataTable before aggregation.<br/>
    ''' <br/>
    ''' The Aggregation rule is defined in AbsAggregatable. So If you want to execute aggregate,there are 2 steps.<br/>
    ''' <u>1.Make class that inherits AbsAggregatable and implements MustOverride.</u><br/>
    ''' <para>
    '''   1.1 Implements createInstance : conversion for DataRow to class instance.<br/>
    '''   1.2 Implements MakeId/MakeCaption : Make id for each depth (For Example, depth 0 is Month , 1 is Customer and Material ... larger depth is more detail ).<br/>
    '''   1.3 Implements Aggregate : Implements aggregation process<br/>
    ''' </para>
    ''' <u>2.Create instance of DataAggregator and execute Aggregate(Of <i>your_class</i>)</u><br/>
    ''' Aggregate's result is List(Of <i>your_class</i>) that includes aggregated(subtotal) record.<br/>
    ''' <br/>
    ''' Aggregation is executed by below process<br/>
    ''' <para>
    ''' 1. Split DataTable by id. Id is made by MakeId and it depends on depth.<br/>
    ''' 2. Aggregate each DataTable by Aggregate(that is implemented in AbsAggregatable ).<br/>
    ''' 3. Merge Aggregated DataTables.<br/>
    ''' </para>
    ''' Note:For split the DataTable, you have to sort DataTable before Aggregation.
    ''' </summary>
    ''' <remarks></remarks>
    Public Class DataAggregator

        Public Sub New(ByVal conName As String)
            _connectionName = conName
        End Sub

        Private _connectionName As String = ""
        Public ReadOnly Property ConnectionName As String
            Get
                Return _connectionName
            End Get
        End Property

        ''' <summary>
        ''' Execute Aggregation
        ''' </summary>
        ''' <typeparam name="T"></typeparam>
        ''' <param name="table">target DataTable</param>
        ''' <param name="id">if you want to aggregate specific part of DataTable, set id of it's part.</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function Aggregate(Of T As AbsAggregatable)(ByRef table As DataTable, Optional ByVal id As String = "") As List(Of T)

            Dim agg As AbsAggregatable = Activator.CreateInstance(Of T)()
            Return Aggregate(Of T)(id, table, agg, 0)

        End Function

        ''' <summary>
        ''' Execute Aggregation . Call myself for recursive process.<br/>
        ''' aggregate depth + 1 aggregated DataTables .
        ''' </summary>
        ''' <typeparam name="T"></typeparam>
        ''' <param name="id"></param>
        ''' <param name="table"></param>
        ''' <param name="abs"></param>
        ''' <param name="depth"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Private Function Aggregate(Of T As AbsAggregatable)(ByVal id As String, ByRef table As DataTable, ByRef abs As T, ByVal depth As Integer) As List(Of T)

            'If aggregation depth over the limit , return Nothing
            If depth >= abs.DepthCount Then
                Return Nothing
            Else

                abs.depth = depth
                Dim myChildren As Dictionary(Of String, DataTable) = getMapById(id, table, abs)
                Dim result As New List(Of T)

                For Each item As KeyValuePair(Of String, DataTable) In myChildren

                    Dim childResult As List(Of T) = Aggregate(item.Key, item.Value, abs, depth + 1)
                    If childResult Is Nothing Then 'The end of node
                        'Add it without aggregation
                        Dim leafs As List(Of AbsAggregatable) = abs.createInstances(item.Value, depth)
                        For Each leaf As AbsAggregatable In leafs
                            leaf.depth = depth
                            leaf.isLeaf = True
                            leaf.parentId = id
                            result.Add(leaf)
                        Next

                    Else
                        'Make subtotal row
                        Dim subtotal As T = abs.createInstance(abs.Aggregate(item.Value), depth)
                        subtotal.depth = depth
                        subtotal.parentId = id
                        subtotal.childCount = item.Value.Rows.Count

                        'Add subtotal row and add details
                        result.Add(subtotal)
                        result.AddRange(childResult)
                    End If
                Next

                Return result
            End If

        End Function

        ''' <summary>
        ''' Get Splited DataTables.
        ''' </summary>
        ''' <param name="id">the id to split</param>
        ''' <param name="table">original DataTable</param>
        ''' <param name="abs"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Private Function getMapById(ByVal id As String, ByRef table As DataTable, ByVal abs As AbsAggregatable) As Dictionary(Of String, DataTable)
            Dim criteria As DataRow = table.NewRow
            abs.MakeCriteriaFromId(id, criteria)

            Dim mapList As Dictionary(Of String, DataTable) = Map(table, criteria, abs)

            Return mapList

        End Function

        ''' <summary>
        ''' Split DataTable by id.<br/>
        ''' Id is made by parameter abs . The value of id depends on it's depth.
        ''' </summary>
        ''' <param name="table">original DataTable</param>
        ''' <param name="criteria">the records which match with this DataRow,is target of aggregation</param>
        ''' <param name="abs"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Private Function Map(ByRef table As DataTable, ByVal criteria As DataRow, ByVal abs As AbsAggregatable) As Dictionary(Of String, DataTable)

            Dim result As New Dictionary(Of String, DataTable)
            Dim tmp As DataTable = table.Clone 'clone structure of datatable
            Dim breakKey As String = ""

            'Extract records whick match with criteria
            Dim query = From row As DataRow In table.AsEnumerable
                        Where abs.IsTarget(criteria, row)
                        Select row

            For i As Integer = 0 To query.Count - 1
                If String.IsNullOrEmpty(breakKey) Then 'First
                    breakKey = abs.MakeId(query(i)) 'Make id of depth now.
                End If

                Dim key As String = abs.MakeId(query(i))
                If Not breakKey.Equals(key) Then 'If id breaks, add id and Datatable(same id records) to result.
                    result.Add(breakKey, tmp)
                    breakKey = key
                    tmp = table.Clone
                End If

                tmp.ImportRow(query(i))

                If i = query.Count - 1 Then 'Last record
                    result.Add(breakKey, tmp)
                End If

            Next

            Return result

        End Function

    End Class


    ''' <summary>
    ''' Abstract Class for Aggregatable Class<br/>
    ''' It's designed to convert Json.
    ''' </summary>
    ''' <remarks></remarks>
    <Serializable()>
    <Runtime.Serialization.DataContract()>
    Public MustInherit Class AbsAggregatable

        <Runtime.Serialization.DataMember()>
        Public Property id As String = String.Empty

        <Runtime.Serialization.DataMember()>
        Public Property caption As String = String.Empty

        <Runtime.Serialization.DataMember()>
        Public Property depth As Integer = 0

        <Runtime.Serialization.DataMember()>
        Public Property isLeaf As Boolean = False

        <Runtime.Serialization.DataMember()>
        Public Property parentId As String = String.Empty

        <Runtime.Serialization.DataMember()>
        Public Property childCount As Integer = 0

        <Runtime.Serialization.DataMember()>
        Protected Shared _keySeparator As String = ","

        Public Shared ReadOnly Property KeySeparator() As String
            Get
                Return _keySeparator
            End Get
        End Property

        Protected _aggrKeys As New List(Of List(Of String))
        Public Function DepthCount() As Integer
            Return _aggrKeys.Count
        End Function

        Protected MustOverride Sub defineAggretation()
        Public MustOverride Function createInstance(ByRef row As DataRow, ByVal depth As Integer) As AbsAggregatable
        Public MustOverride Function MakeCaption(ByRef row As DataRow, ByVal depth As Integer) As String
        Public MustOverride Sub Aggregate(ByRef target As DataTable, ByRef toRow As DataRow)

        Public Sub New()
            defineAggretation()
        End Sub

        Public Function createInstances(ByRef table As DataTable, ByVal depth As Integer) As List(Of AbsAggregatable)
            Dim result As New List(Of AbsAggregatable)
            For Each row As DataRow In table.Rows
                result.Add(createInstance(row, depth))
            Next
            Return result
        End Function

        Public Function ListupKeys(ByVal depth As Integer) As List(Of String)
            Dim result As New List(Of String)
            Dim depthVal As Integer = depth

            For i As Integer = 0 To depthVal
                If depthVal < _aggrKeys.Count Then
                    result.AddRange(_aggrKeys(i))
                End If
            Next

            Return result

        End Function

        Public Function GetKeyValue(ByVal keyName As String) As String
            Dim values As String() = Split(Me.id, KeySeparator)
            Dim columns As List(Of String) = ListupKeys(Me.depth)
            Dim pos As Integer = columns.IndexOf(keyName)

            If pos > -1 AndAlso pos < values.Count Then
                Return values(pos)
            Else
                Return String.Empty
            End If

        End Function

        Public Function makeReplacedKey(ByVal keyName As String, ByVal changedValue As String) As String
            Dim values As String() = Split(Me.id, KeySeparator)
            Dim columns As List(Of String) = ListupKeys(Me.depth)
            Dim pos As Integer = columns.IndexOf(keyName)

            If pos > -1 AndAlso pos < values.Count Then
                If Not changedValue Is Nothing Then
                    values(pos) = changedValue
                Else
                    Dim erased As List(Of String) = values.ToList 'If value is Nothing, delete the element
                    erased.RemoveAt(pos)
                    values = erased.ToArray
                End If
                Return String.Join(KeySeparator, values)
            Else
                Return id
            End If

        End Function

        Public Function Aggregate(ByRef target As DataTable) As DataRow
            Dim aggrTbl As DataTable = target.Clone
            Dim aggrRow As DataRow = aggrTbl.NewRow

            Aggregate(target, aggrRow)

            aggrTbl.Rows.Add(aggrRow)

            Return aggrTbl(0)
        End Function

        Public Overridable Function MakeId(ByRef row As DataRow) As String
            Return MakeIdImpl(row, Me.depth)
        End Function
        Protected Function MakeIdImpl(ByRef row As DataRow, ByVal depth As Integer) As String

            Dim keys As New List(Of String)
            Dim keyList As List(Of String) = ListupKeys(depth)
            For Each name As String In keyList
                keys.Add(row(name).ToString)
            Next
            Return String.Join(KeySeparator, keys)

        End Function

        Public Overridable Function IsTarget(ByRef crRow As DataRow, ByRef row As DataRow) As Boolean
            If depth > 0 Then
                Return MakeIdImpl(crRow, depth - 1) = MakeIdImpl(row, depth - 1)
            Else
                Return True 'First,all row is target
            End If
        End Function
        Public Overridable Sub MakeCriteriaFromId(ByVal id As String, ByRef row As DataRow)
            If depth > 0 Then
                'Make parent's criteria
                Dim element As String() = Split(id, KeySeparator)
                Dim columns As List(Of String) = ListupKeys(depth - 1)
                For i As Integer = 0 To columns.Count - 1
                    row(columns(i)) = element(i)
                Next
            End If
        End Sub

        Public Overridable Function toValue(ByVal value As Object) As Decimal
            Dim d As Decimal = 0
            If Not (IsDBNull(value) Or value Is Nothing) Then
                If Not TypeOf value Is Decimal Then
                    If Decimal.TryParse(value.ToString, d) Then
                    End If
                Else
                    d = value
                End If
            End If
            Return d
        End Function

        Protected Function IsEqual(ByVal criteria As Object, ByVal matchValue As Object, Optional ByVal ignoreWhenCriteriaIsSpace As Boolean = False) As Boolean
            Dim result As Boolean = False

            If ignoreWhenCriteriaIsSpace And (Not criteria Is Nothing AndAlso criteria.ToString = "") Then
                Return True
            End If

            If criteria Is Nothing Then
                If String.IsNullOrEmpty(matchValue) Or IsDBNull(matchValue) Then 'Nothing is equal to DBnull
                    result = True
                End If
            Else
                If criteria.Equals(matchValue) Then
                    result = True
                End If
            End If
            Return result
        End Function

    End Class

End Namespace

