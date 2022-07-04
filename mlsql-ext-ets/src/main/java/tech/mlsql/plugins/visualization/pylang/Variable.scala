package tech.mlsql.plugins.visualization.pylang

import scala.collection.mutable.ArrayBuffer

// PyLang().variable("df").invokeFunc("to_pandas").params.add("name","").end
// ray_context.to_pandas()
class VariableFunc[T](val v: Variable[T], val name: String) {
  private[pylang] var o: Options[T] = null

  def params = {
    o = new Options[T](v)
    o
  }

  def end = {
    v
  }
}

class VariableObjectCreate[T](val v: Variable[T]) {
  private[pylang] var _name: String = null
  private[pylang] var _extend: Option[String] = None

  def addImport(s: String) = {
    v.lang.imports += s
    this
  }

  def create(s: String) = {
    _name = s
    this
  }

  def extend(s: String) = {
    _extend = Some(s)
    this
  }

  def withParameters(params: List[Literal]) = {
    this
  }

  def end = {
    v
  }
}

// PythonLang().let("abc").bind.str().end
class Variable[T](val lang: PyLang, val parent: T, _name: String) extends BaseNode[T] {
  private var binder: Option[Binder[T]] = None
  private var _invokeFuncs = ArrayBuffer[VariableFunc[T]]()
  private var _invokeObjectCreate: Option[VariableObjectCreate[T]] = None

  private var _tag: Option[String] = None
  private var _variableName = _name
  private var _newVariableName: Option[String] = None

  private var _ident = ""

  override def variableName: String = _variableName

  override def namedVariableName(variableName: String): BaseNode[T] = {
    _newVariableName = Some(variableName)
    this
  }

  def bind = {
    binder = Some(new Binder[T](this))
    binder.get
  }

  def invokeFunc(name: String) = {
    val v = new VariableFunc[T](this, name)
    _invokeFuncs += v
    v
  }

  def invokeObjectCreate = {
    val v = new VariableObjectCreate[T](this)
    _invokeObjectCreate = Some(v)
    v
  }

  def invokeField() = {
  }

  def end: T = {
    _ident = lang.ident
    parent
  }

  override def tag(str: String): BaseNode[T] = {
    _tag = Some(str)
    this
  }

  override def options(): Options[T] = ???

  override def toBlock: String = {
    if (binder.isDefined) {
      return s"""${_ident}${_name} = ${binder.get.value.toBlock}"""
    }
    if (!_invokeFuncs.isEmpty) {
      val funcs = _invokeFuncs.map { item =>
        val parameters = if (item.o != null) {
          item.o.toFragment
        } else ""
        s"""${_ident}${item.name}(${parameters})"""
      }.mkString(".")

      if (_newVariableName.isDefined) {
        return s"""${_ident}${_newVariableName.get} = ${_name}.${funcs}"""
      } else {
        return s"""${_ident}${_name}.${funcs}"""
      }
    }

    if (_invokeObjectCreate.isDefined) {
      val v = _invokeObjectCreate.get
      if (_newVariableName.isDefined) {
        return s"""${_ident}${_newVariableName.get} = ${v._name}()"""
      } else {
        return s"""${_ident}${v}()"""
      }
    }

    ""
  }

  override def getTag: Option[String] = _tag

  override def toJson: String = ???

  override def fromJson(json: String): BaseNode[T] = ???

  override def block(): PyLang = ???
}
