package tech.mlsql.plugins.visualization.pylang


class RawCode[T](val lang: PyLang, val parent: T) extends BaseNode[T] {


  private var _tag: Option[String] = None
  private var _funcName = ""
  private var _newFuncName: Option[String] = None

  private var _ident = lang.ident

  private var _code = ""

  override def variableName: String = _funcName

  override def namedVariableName(funcName: String): BaseNode[T] = {
    _newFuncName = Some(funcName)
    this
  }

  def code(s: String) = {
    _code = s
    this
  }


  def end: T = {
    parent
  }

  override def tag(str: String): BaseNode[T] = {
    _tag = Some(str)
    this
  }

  override def options(): Options[T] = ???

  override def toBlock: String = {
    s"${_ident}${_code}"
  }

  override def getTag: Option[String] = _tag

  override def toJson: String = ???

  override def fromJson(json: String): BaseNode[T] = ???

  override def block(): PyLang = ???
}
