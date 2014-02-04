Imports System.Data.Common
Imports DataBaseExecutors

<TestClass()>
Public Class OracleParameterTest

    Private Const OracleConnection As String = "OracleConnect"

    <TestMethod()>
    Public Sub useXMLType()
        Dim db As New DBExecution(OracleConnection)

        'Function作成
        db.sqlExecution("CREATE FUNCTION DBExecutionXML(xml XMLTYPE) RETURN XMLTYPE IS BEGIN RETURN xml; END;")

        Dim xml As String = "<DATA><ITEM attr=""NUMBER"">1</ITEM><ITEM attr=""TEXT"">アイテム</ITEM></DATA>"
        Dim paramXml As New DBExecutionParameter("pXml", DbType.Xml)
        paramXml.Value = xml '渡すときはXMLドキュメント型でなく、String型で渡す

        Dim resultXml As New DBExecutionParameter("rXml", DbType.Xml)
        resultXml.Direction = ParameterDirection.ReturnValue

        db.addFilter(paramXml)
        db.addFilter(resultXml)

        Dim result As Xml.XmlDocument = db.executeDBFunction(Of Xml.XmlDocument)("DBExecutionXML")

        Dim expected As New Xml.XmlDocument()
        expected.LoadXml(xml)

        Assert.AreEqual(expected.InnerXml.Replace(" ", "").Replace(vbLf, ""), result.InnerXml.Replace(" ", "").Replace(vbLf, ""))

        'Functionのドロップ
        db.sqlExecution("DROP FUNCTION DBExecutionXML")

    End Sub

    <TestMethod()>
    Public Sub useGUIDType()
        Dim db As New DBExecution(OracleConnection)

        'Function作成
        db.sqlExecution("CREATE FUNCTION DBExecutionGUID(id RAW) RETURN RAW IS BEGIN RETURN id; END;")

        Dim id As Guid = Guid.NewGuid
        Dim expected As New Guid(id.ToByteArray)
        Dim paramId As New DBExecutionParameter("pId", DbType.Guid)
        paramId.Value = id

        Dim resultId As New DBExecutionParameter("rId", DbType.Guid)
        resultId.Direction = ParameterDirection.ReturnValue

        db.addFilter(paramId)
        db.addFilter(resultId)

        Dim result As Guid = db.executeDBFunction(Of Guid)("DBExecutionGUID")

        Assert.AreEqual(expected, result)

        'Functionのドロップ
        db.sqlExecution("DROP FUNCTION DBExecutionGUID")

    End Sub

    <TestMethod()>
    Public Sub useCLOBType()
        Dim db As New DBExecution(OracleConnection)

        'Function作成
        db.sqlExecution("CREATE FUNCTION DBExecutionCLOB(text CLOB) RETURN CLOB IS BEGIN RETURN text; END;")

        Dim text As String = "X"
        For i As Integer = 0 To 10000
            text += "X"
        Next

        Dim resultText As New DBExecutionParameter("rText", DbType.AnsiString)
        resultText.Direction = ParameterDirection.ReturnValue
        resultText.Size = text.Length

        db.addFilter("text", text)
        db.addFilter(resultText)

        Dim result As String = db.executeDBFunction(Of String)("DBExecutionCLOB")

        Assert.AreEqual(text, result)

        'Functionのドロップ
        db.sqlExecution("DROP FUNCTION DBExecutionCLOB")

    End Sub


End Class
