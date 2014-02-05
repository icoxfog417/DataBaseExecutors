Imports DataBaseExecutors
Imports System.Reflection

Namespace DataBaseExecutors.Entity

    ''' <summary>
    ''' Attribute for setting database's table/view name of class.
    ''' </summary>
    ''' <remarks></remarks>
    Public Class DBSourceAttribute
        Inherits System.Attribute
        Public Property Table As String
        Public Property View As String
    End Class

    ''' <summary>
    ''' Attribute for setting database's column information.
    ''' </summary>
    ''' <remarks></remarks>
    Public Class DBColumnAttribute
        Inherits System.Attribute

        ''' <summary>Database column name or property</summary>
        Public Property Name As String

        ''' <summary>Is table key or not</summary>
        Public Property IsKey As Boolean = False

        ''' <summary>Order no when executing sql (use it when composite key)</summary>
        Public Property Order As Integer = 0

        ''' <summary>DateTime format on database. If you store datetime as String in database, use this property.</summary>
        Public Property Format As String = ""

        ''' <summary>If the property's value is set in database automatically(like sequential number), set true</summary>
        Public Property IsDbGenerate As Boolean = False
    End Class

    ''' <summary>
    ''' The class to store DBColumn and Property information.
    ''' </summary>
    ''' <remarks></remarks>
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
                Return _value
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

        ''' <summary>
        ''' Parameter name for parameter query
        ''' </summary>
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
