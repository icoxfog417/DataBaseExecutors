Imports Microsoft.VisualBasic
Imports System.Data
Imports System.Data.Common
Imports System.Collections.Generic
Imports DataBaseExecutors.Adapter

Namespace DataBaseExecutors

    ''' <summary>
    ''' The class for sql query parameter.
    ''' </summary>
    ''' <remarks></remarks>
    Public Class DBExecutionParameter
        Inherits DbParameter
        Public Const DEFAULT_LENGTH As Integer = 2000

        Private _dbType As DbType = DbType.AnsiString
        Private _paramDirection As ParameterDirection = ParameterDirection.Input
        Private _isNullable As Boolean
        Private _value As Object
        Private _paramName As String
        Private _sColumn As String
        Private _size As Integer = DEFAULT_LENGTH
        Private _scNullMap As Boolean
        Private _sourceVersion As System.Data.DataRowVersion
        Private isDirty As New List(Of String)

        Public Sub New(Optional ByVal withdefault As Boolean = True)
            MyBase.New()
            If withdefault Then
                setDirty("DbType")
                setDirty("Direction")
                setDirty("Size")
            End If
        End Sub
        Public Sub New(ByVal pName As String, ByVal type As DbType)
            MyBase.New()
            ParameterName = pName
            DbType = type
        End Sub
        Public Sub New(ByVal pName As String, ByVal type As DbType, ByVal v As Object)
            Me.New(pName, type)
            Value = v

        End Sub

        ''' <summary>
        ''' set isDirty when parameter is changed from default
        ''' </summary>
        ''' <param name="pname"></param>
        ''' <remarks></remarks>
        Private Sub setDirty(ByVal pname As String)
            If isDirty.IndexOf(pname) = -1 Then
                isDirty.Add(pname)
            End If
        End Sub

        ''' <summary>
        ''' The Type of parameter
        ''' </summary>
        Public Overrides Property DbType As System.Data.DbType
            Get
                Return _dbType
            End Get
            Set(ByVal value As System.Data.DbType)
                _dbType = value
                setDirty("DbType")
            End Set
        End Property

        ''' <summary>
        ''' The Direction of parameter
        ''' </summary>
        Public Overrides Property Direction As System.Data.ParameterDirection
            Get
                Return _paramDirection
            End Get
            Set(ByVal value As System.Data.ParameterDirection)
                _paramDirection = value
                setDirty("Direction")
            End Set
        End Property

        ''' <summary>
        ''' Is parameter nullable or not.
        ''' </summary>
        Public Overrides Property IsNullable As Boolean
            Get
                Return _isNullable
            End Get
            Set(ByVal value As Boolean)
                _isNullable = value
                setDirty("IsNullable")
            End Set
        End Property

        ''' <summary>
        ''' Parameter's name
        ''' </summary>
        Public Overrides Property ParameterName As String
            Get
                Return _paramName
            End Get
            Set(ByVal value As String)
                _paramName = value
                setDirty("ParameterName")
            End Set
        End Property

        Public Overrides Sub ResetDbType()
        End Sub

        ''' <summary>
        ''' The size of parameter.<br/>
        ''' If it's not set, set size by Value's size.
        ''' </summary>
        Public Overrides Property Size As Integer
            Get
                Return _size
            End Get
            Set(ByVal value As Integer)
                _size = value
                setDirty("Size")
            End Set
        End Property

        Public Overrides Property SourceColumn As String
            Get
                Return _sColumn
            End Get
            Set(ByVal value As String)
                _sColumn = value
                setDirty("SourceColumn")
            End Set
        End Property

        Public Overrides Property SourceColumnNullMapping As Boolean
            Get
                Return _scNullMap
            End Get
            Set(ByVal value As Boolean)
                _scNullMap = value
                setDirty("SourceColumnNullMapping")
            End Set
        End Property

        Public Overrides Property SourceVersion As System.Data.DataRowVersion
            Get
                Return _sourceVersion
            End Get
            Set(ByVal value As System.Data.DataRowVersion)
                _sourceVersion = value
                setDirty("SourceVersion")
            End Set
        End Property

        ''' <summary>
        ''' Value of parameter.
        ''' </summary>
        Public Overrides Property Value As Object
            Get
                Return _value
            End Get
            Set(ByVal value As Object)
                _value = value
                setDirty("Value")

                If TypeOf value Is String AndAlso value.ToString.Length > Size Then
                    Size = value.ToString.Length
                ElseIf TypeOf value Is Array Then
                    Size = CType(value, Array).Length
                End If

            End Set
        End Property

        ''' <summary>
        ''' Set each parameters to received DbParameter.<br/>
        ''' Only the edited(dirtied) parameter is set to received DbParameter (for keeping default value of DbParameter).<br/>
        ''' When set the parameter, convert value by using adapter.
        ''' </summary>
        ''' <param name="param"></param>
        ''' <param name="adapter"></param>
        ''' <remarks></remarks>
        Public Sub transferData(ByRef param As DbParameter, ByRef adapter As AbsDBParameterAdapter)

            'transfer setting process to adapter when database peculiarity parameter
            For Each pName As String In isDirty
                Select Case pName
                    Case "DbType"
                        If Not adapter Is Nothing Then
                            adapter.SetDbType(Me, param)
                        Else
                            param.DbType = DbType
                        End If
                    Case "Direction"
                        param.Direction = Direction
                    Case "IsNullable"
                        param.IsNullable = IsNullable
                    Case "ParameterName"
                        param.ParameterName = ParameterName
                    Case "Size"
                        param.Size = Size
                    Case "SourceColumn"
                        param.SourceColumn = SourceColumn
                    Case "SourceColumnNullMapping"
                        param.SourceColumnNullMapping = SourceColumnNullMapping
                    Case "SourceVersion"
                        param.SourceVersion = SourceVersion
                    Case "Value"
                        If Not adapter Is Nothing Then
                            adapter.SetValue(Me, param)
                        Else
                            param.Value = Value
                        End If
                End Select
            Next

            If isDirty.IndexOf("Size") = -1 And isDirty.IndexOf("Value") > -1 Then
                param.Size = param.Value.ToString.Length
            End If

        End Sub

    End Class

End Namespace
