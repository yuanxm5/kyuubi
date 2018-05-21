package yaooqinn.kyuubi.user

import java.util.concurrent.ConcurrentHashMap

import yaooqinn.kyuubi.Logging
import scala.collection.JavaConverters._



/**
  * yaooqinn.kyuubi.user.UserInfo
  *
  * @author xiangmin
  * @since 2018/5/17
  */
class UserInfoManager extends Logging {
  private[this] val userToUserInfo = new ConcurrentHashMap[String, UserInfo]

  def setIfNotExist(username: String, userInfo: UserInfo) = {
    Option(userToUserInfo.get(username)) match {
      case Some(u) =>
      case _ => userToUserInfo.put(username, userInfo)
    }
  }

  def remove(username: String) = {
    userToUserInfo.remove(username)
  }

  def getUserInfo(username: String): Option[UserInfo] = {
    userToUserInfo.asScala.get(username)
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
