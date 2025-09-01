package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    public StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id) {
        /*String key = "cache:shop:" + id;
        //1.从redis查询商户缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);  //查询到的是Json对象
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3.存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return Result.ok(shop);
        }

        if(shopJson != null){
            //返回一个错误信息
            return Result.fail("店铺不存在");
        }

        //4.不存在，根据id查询数据库
        Shop shop = getById(id);

        //5.数据库不存在， 返回错误
        if (shop == null) {
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", 2, TimeUnit.MINUTES);
            return Result.fail("店铺不存在");
        }
        //6.存在，写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop));*/

        //缓存穿透
        //Shop shop = queryWithPassThrough(id);

        //缓存击穿
        Shop shop = queryWithMutex(id);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        //7.返回
        return Result.ok(shop);
    }

    //返回空值，缓存穿透
    public Shop queryWithPassThrough(Long id) {
        String key = "cache:shop:" + id;
        //1.从redis查询商户缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);  //查询到的是Json对象
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3.存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }

        if(shopJson != null){
            //返回一个错误信息
            return null;
        }

        //4.不存在，根据id查询数据库
        Shop shop = getById(id);

        //5.数据库不存在， 返回错误
        if (shop == null) {
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", 2, TimeUnit.MINUTES);
            return null;
        }
        //6.存在，写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop));
        //7.返回
        return shop;
    }

    //缓存击穿
    public Shop queryWithMutex(Long id) {
        String key = "cache:shop:" + id;
        //1.从redis查询商户缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);  //查询到的是Json对象
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3.存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }

        if(shopJson != null){
            //返回一个错误信息
            return null;
        }

        //4.尝试获取互斥锁
        String lockKey = "lock:shop:" + id;
        Shop shop = null;
        try{
            boolean isLock = tryLock(lockKey);
            //4.1 判断是否获取成功
            if(!isLock){
                //4.2 失败，则休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //4.3 成功根据id查询数据库
            shop = getById(id);
            //模拟重建的延时
            Thread.sleep(200);

            //5.数据库不存在， 返回错误
            if (shop == null) {
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(key, "", 2, TimeUnit.MINUTES);
                return null;
            }
            //6.存在，写入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop));
        } catch (InterruptedException e){
            throw new RuntimeException(e);
        }finally {
            //6.1 释放互斥锁
            unlock(lockKey);
        }
        //7.返回
        return shop;
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete("cache:shop:" + id);
        return Result.ok();
    }

    //互斥锁
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
}
