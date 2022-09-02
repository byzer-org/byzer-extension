package tech.mlsql.plugins.render

import tech.mlsql.runtime.AppRuntimeStore
import tech.mlsql.version.VersionCompatibility

class ExceptionReplaceRenderApp extends tech.mlsql.app.App with VersionCompatibility {
  override def run(args: Seq[String]): Unit = {
    AppRuntimeStore.store.registerExceptionRender("ExceptionReplaceRender", classOf[ExceptionReplaceRender].getName)
  }

  override def supportedVersions: Seq[String] = {
    Seq(">=2.0.0")
  }
}
