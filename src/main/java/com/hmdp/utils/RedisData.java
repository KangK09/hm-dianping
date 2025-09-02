package com.hmdp.utils;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class RedisData {
    //逻辑过期
    private LocalDateTime expireTime;    //第一种让shop来继承
    private Object data;   //在这个object data里面存数据
}
