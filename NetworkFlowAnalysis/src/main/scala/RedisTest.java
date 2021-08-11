import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.Set;

/**
 * @author Conway
 * @date 2021/1/15 21:04
 */
public class RedisTest {
    public static void main(String[] args) {

                //连接本地的 Redis 服务
                Jedis jedis = new Jedis("localhost");
                // 如果 Redis 服务设置来密码，需要下面这行，没有就不需要
                // jedis.auth("123456");

        System.out.println("连接成功");
                 //查看服务是否运行
        System.out.println("服务正在运行: "+jedis.ping());

        // 获取数据并输出
        Set<String> keys = jedis.keys("*");
        Iterator<String> it=keys.iterator() ;
        while(it.hasNext()){
            String key = it.next();
            System.out.println(key);
        }

    }
}
