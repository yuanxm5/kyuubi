package yaooqinn.kyuubi.user

import java.util
import java.util.concurrent.ConcurrentHashMap

import yaooqinn.kyuubi.Logging
import scala.collection.JavaConverters._
import org.apache.spark.KyuubiSparkUtil._
import org.apache.spark.KyuubiConf._

/**
  * yaooqinn.kyuubi.user.UserInfo
  *
  * @author xiangmin
  * @since 2018/5/17
  */
class UserInfoManager extends Logging {
  private[this] val userToUserInfo = new ConcurrentHashMap[String, UserInfo]
  private[this] val accessableUsers = new util.HashSet[String]


  def setIfNotExist(username: String, userInfo: UserInfo) = {
    accessableUsers.add(username)
    Option(userToUserInfo.get(username)) match {
      case Some(u) =>
        userInfo.getConf.get(HIVE_VAR + OVERWRITE_LAST_SESSION_CONF.key) match {
          case Some(ov) if (ov.toBoolean) =>
            info(s"Overwrite session conf for user: $username, conf: ${userInfo.getConf}")
            userToUserInfo.put(username, userInfo)
          case _ =>
        }
      case _ => userToUserInfo.put(username, userInfo)
    }
  }

  def remove(username: String) = {
    accessableUsers.remove(username)
    userToUserInfo.remove(username)
  }

  def getUserInfo(username: String): Option[UserInfo] = {
    userToUserInfo.asScala.get(username)
  }

  def isAccessable(username: String): Boolean ={
    accessableUsers.contains(username)
  }
}

object UserInfoManager extends Logging {
  private[this] var userInfoManager: UserInfoManager = _
  def start() = {
    userInfoManager = new UserInfoManager
  }
  def get = userInfoManager
}

class UserInfo(sessionConf: Map[String, String]) {
  var recentSqls: Array[String] = _
  def getConf = sessionConf
}
