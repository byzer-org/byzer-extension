package tech.mlsql.plugins.llm.utils

object LLMUtils {
  case class SplitInfo(content: String, content_id: Int)

  def content_split(content: String, splits: Seq[String], limits: Int): Seq[SplitInfo] = {
    var output = Seq[SplitInfo]()
    var start = 0
    if (content.length <= limits) {
      output = output :+ SplitInfo(content, 0)
    } else {
      var end = limits
      while (start < content.length) {
        val part = content.substring(start, end)
        val validEnd = splits.map(s => part.lastIndexOf(s)).max
        if (validEnd == -1) {
          output = output :+ SplitInfo(part, output.length)
        } else {
          output = output :+ SplitInfo(part.substring(0, validEnd + 1), output.length)
          end = start + validEnd + 1
        }
        start = end
        end = if (start + limits < content.length) start + limits else content.length
      }
    }
    output
  }
}
