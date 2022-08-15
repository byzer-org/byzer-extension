package tech.mlsql.plugins.visualization.pylang

import scala.collection.mutable.ArrayBuffer

// PythonLang().obj("context").invoke("to_pandas").withParameters(....).end
// df=context.to_pandas()
object PyLang {
  def apply(): PyLang = new PyLang()
}

class PyLang {
  private[visualization] var imports = ArrayBuffer[String]()
  private[visualization] var blocks = ArrayBuffer[BaseNode[_]]()
  private[visualization] var current_ident = 0

  private[pylang] def forward = {
    current_ident += 1
    this
  }

  private[pylang] def back = {
    current_ident -= 1
    this
  }

  private[pylang] def ident: String = {
    " " * current_ident * 4
  }

  def raw = {
    val v = new RawCode[PyLang](this, this)
    blocks += v
    v
  }

  def let(name: String) = {
    val v = new Variable[PyLang](this, this, name)
    blocks += v
    v
  }

  def func(name: String) = {
    val v = new Func[PyLang](this, this, name)
    blocks += v
    v
  }

  def getByTag(name: String): List[BaseNode[_]] = {
    blocks.filter(_.getTag.isDefined).filter(_.getTag.get == name).toList
  }

  def toScript: String = {
    val _imports = imports.mkString("\n")
    val _blocks = blocks.map(item => item.toBlock).mkString("\n")
    if (!imports.isEmpty) {
      _imports + "\n\n\n" + _blocks
    } else {
      _blocks
    }

  }
}

case class OptionKeyValue(name: Option[String], value: String, quoteStr: Option[String])

class Options[T](parent: BaseNode[T]) {
  private val o = ArrayBuffer[OptionKeyValue]()

  def add(value: String, quoteStr: Option[String]): Options[T] = {
    o += OptionKeyValue(None, value, quoteStr)
    this
  }

  def addKV(name: String, value: String, quoteStr: Option[String]) = {
    o += OptionKeyValue(Some(name), value, quoteStr)
    this
  }

  def end = {
    parent
  }

  def toFragment = {
    val opts = o.map { case kv =>

      def getValue = {
        if (kv.quoteStr.isEmpty) {
          kv.value
        } else {
          s"""${kv.quoteStr.get}${kv.value}${kv.quoteStr.get}"""
        }
      }

      def getKey = {
        if (kv.name.isEmpty) {
          ""
        } else {
          s"""${kv.name.get}="""
        }
      }

      s"${getKey}${getValue}"

    }.mkString(" , ")

    val optStr = if (!o.isEmpty) {
      s"""${opts}"""
    } else {
      ""
    }
    optStr
  }
}

trait BaseNode[T] {
  def variableName: String

  def namedVariableName(variableName: String): BaseNode[T]

  def tag(str: String): BaseNode[T]

  def options(): Options[T]

  def toBlock: String

  def getTag: Option[String]

  def block(): PyLang

  def toJson: String

  def end: T

  def fromJson(json: String): BaseNode[T]
}

trait Literal {
  def toBlock: String
}

class Str[T](s: String, parent: Variable[T]) extends Literal {
  private var quoteStr = "\""

  def quoted(s: String) = {
    quoteStr = s
    this
  }

  override def toBlock: String = {
    s"""${quoteStr}${s}${quoteStr}"""
  }

  def end = {
    parent
  }
}

class Number[T](s: String, parent: Variable[T]) extends Literal {
  override def toBlock: String = {
    s"""${s}"""
  }

  def end = {
    parent
  }
}

class Boolean[T](s: String, parent: Variable[T]) extends Literal {
  override def toBlock: String = {
    s"""${s}"""
  }

  def end = {
    parent
  }
}

class ReferenceVariable[T](s: String, parent: Variable[T]) extends Literal {
  override def toBlock: String = {
    s"""${s}"""
  }

  def end = {
    parent
  }
}

class Binder[T](parent: Variable[T]) {
  private[pylang] var value: Literal = null

  def str(s: String) = {
    val v = new Str[T](s, parent)
    value = v
    v
  }

  def number(s: String) = {
    val v = new Number[T](s, parent)
    value = v
    v
  }

  def boolean(s: String) = {
    val v = new Boolean[T](s, parent)
    value = v
    v
  }

  def variable(s: String) = {
    val v = new ReferenceVariable[T](s, parent)
    value = v
    v
  }
}



