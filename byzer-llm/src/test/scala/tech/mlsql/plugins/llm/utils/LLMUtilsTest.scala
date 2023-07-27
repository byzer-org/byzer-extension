package tech.mlsql.plugins.llm.utils

import org.scalatest.funsuite.AnyFunSuiteLike

class LLMUtilsTest extends AnyFunSuiteLike {

  def test_content_split = {
    var content = "hello,\nnihao"
    var output = LLMUtils.content_split(content, Seq("\n"), 500);
    assert(output.map(_.content).mkString("") == content)
    assert(output.length == 1)

    content = "hello,\nnihao\nnihao"
    output = LLMUtils.content_split(content, Seq("\n"), 5);
    assert(output.map(_.content).mkString("") == content)
    assert(output.length == 5)

    content = "hello,\nnihao.nihao"
    output = LLMUtils.content_split(content, Seq("\n", "."), 6);
    assert(output.map(_.content).mkString("") == content)
    assert(output.length == 4)

    // 读取Readme.md
    content = scala.io.Source.fromFile("README.md").mkString
    output = LLMUtils.content_split(content, Seq("\n"), 500);
    assert(output.map(_.content).mkString("") == content)
  }
}
