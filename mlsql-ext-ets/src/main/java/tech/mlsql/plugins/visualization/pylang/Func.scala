package tech.mlsql.plugins.visualization.pylang

/**
 * def add(a,b):
 * r = a + b
 * return r
 *
 * PyLang().func.name("add").
 * parameters.field("a").field("b").end.
 * block.
 * let("r").bind.field("a + b").end.
 * end
 */
class Func[T](val lang: PyLang, val parent: T, _name: String) extends BaseNode[T] {


  private var _tag: Option[String] = None
  private var _funcName = _name
  private var _newFuncName: Option[String] = None

  private var _ident = lang.ident

  override def variableName: String = _funcName

  override def namedVariableName(funcName: String): BaseNode[T] = {
    _newFuncName = Some(funcName)
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
    s"${_ident}def ${_funcName}():"
  }

  override def block(): PyLang = {
    lang.forward
  }

  override def getTag: Option[String] = _tag

  override def toJson: String = ???

  override def fromJson(json: String): BaseNode[T] = ???


}

class Block[T](lang: PyLang, parent: T) {
  def end = {
    lang.back
    parent
  }

  def let(name: String) = {
    val v = new Variable[Block[T]](lang, this, name: String)
    lang.blocks += v
    v
  }
}
