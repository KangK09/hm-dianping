package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

//redis lock
public class SimpleRedisLock implements ILock{

    //名称不能写死，不让任何业务进来获取的是同一把锁，不同业务需要不同的锁
    private String name;  //业务名称

    private StringRedisTemplate stringRedisTemplate;

    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    private static final String key_Prefix = "lock:";  //前缀
    private static final String ID_Prefix = UUID.randomUUID().toString(true) + "-"; //ID前缀

    //初始化一个 redis lua脚本对象
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();        //创建了一个redis 脚本执行对象
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua")); //指定lua脚本文件的位置  unlock.lua在项目的resources目录下
        UNLOCK_SCRIPT.setResultType(Long.class);   //声明返回值类型
    }

    //获取锁
    @Override
    public boolean tryLock(long timeoutSec) {
        //获取线程id
        String threadId = ID_Prefix + Thread.currentThread().getId();
        //获取锁
        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(
                key_Prefix+name, threadId, timeoutSec, TimeUnit.SECONDS);

        return Boolean.TRUE.equals(success);
    }


    //释放锁
    @Override
    public void unlock() {
        //调用lua脚本
        stringRedisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(key_Prefix+name),
                ID_Prefix + Thread.currentThread().getId());
    }


    /*//释放锁
    @Override
    public void unlock() {
        //获取线程标识
        String threadId = ID_Prefix + Thread.currentThread().getId();
        //获取锁中的标识
        String id = stringRedisTemplate.opsForValue().get(key_Prefix+name);
        //判断标识是否一致
        if(threadId.equals(id)) {
            //释放锁
            stringRedisTemplate.delete(key_Prefix+name);
        }

    }*/
}
