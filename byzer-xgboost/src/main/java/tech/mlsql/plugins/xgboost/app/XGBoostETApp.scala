package tech.mlsql.plugins.xgboost.app


import tech.mlsql.common.utils.log.Logging
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.plugins.xgboost.ets.{SQLXGBoostClassifier, SQLXGBoostRegressor}
import tech.mlsql.version.VersionCompatibility

/**
 * @author tangfei(tangfeizz@outlook.com)
 * @date 2022/9/27 14:29
 */
class XGBoostETApp extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("XGBoostClassifier", classOf[SQLXGBoostClassifier].getName)
    ETRegister.register("XGBoostRegressor", classOf[SQLXGBoostRegressor].getName)
  }


  override def supportedVersions: Seq[String] = {
    XGBoostETApp.versions
  }
}

object XGBoostETApp {
  val versions = Seq(">=2.0.0")
}
