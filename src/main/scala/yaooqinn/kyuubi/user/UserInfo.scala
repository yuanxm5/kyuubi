package yaooqinn.kyuubi.user

import java.util.concurrent.ConcurrentHashMap

/**
  * yaooqinn.kyuubi.user.UserInfo
  *
  * @author xiangmin
  * @since 2018/5/17
  */
class UserInfoManager {
  private[this] var userToUserInfo: ConcurrentHashMap[String, UserInfo] = _
}

class UserInfo(username: String) {
  private[this] var sessionRecycleTime: Int = _
  private [this] var recentSqls: Array[String] = _

}

object UserInfo{
  def main(args: Array[String]) {
    println("aaa")
  }
}
