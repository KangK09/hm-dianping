package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;


    //初始化一个 redis lua脚本对象
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();        //创建了一个redis 脚本执行对象
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua")); //指定lua脚本文件的位置  unlock.lua在项目的resources目录下
        SECKILL_SCRIPT.setResultType(Long.class);   //声明返回值类型
    }

    //负责存， 阻塞队列起缓冲的作用
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    //负责干活，保存到数据库等操作
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (true) {
                //1.获取队列中的订单信息
                try {
                    VoucherOrder voucherOrder = orderTasks.take();
                    handleVoucherOrder(voucherOrder);
                } catch (InterruptedException e) {
                    log.error("处理订单异常", e);
                }

                //2.创建订单
            }
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //1.获取用户
        Long userId = voucherOrder.getUserId();
        //用redisson 创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        //获取锁
        //boolean isLock = redisLock.tryLock(1200);

        boolean isLock = lock.tryLock();
        //判断释放获取成功
        if(!isLock){
            //获取锁失败，返回错误或重试
            log.error("不允许重复下单");
            return;
        }
        try {
            //proxy.createVocherOrder(voucherOrder);
            voucherOrderService.createVocherOrder(voucherOrder);
        } finally {
            //释放锁
            lock.unlock();
        }
    }

    //private IVoucherOrderService proxy;

    @Override
    public Result seckillVocher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );

        //2.判断结果是否为0
        int r = result.intValue();
        if (r != 0) {
            //2.1不为0， 代表没用购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        //2.2 为0， 代表有购买资格，把下单信息保存到阻塞队列
        //保存阻塞队列
        //6.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //6.1 订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //6.2 用户id
        voucherOrder.setUserId(userId);
        //6.3 代金卷id
        voucherOrder.setVoucherId(voucherId);

        //放入阻塞队列
        orderTasks.add(voucherOrder);

        //获取代理对象
        //proxy = (IVoucherOrderService) AopContext.currentProxy();

        //3.返回订单id
        return Result.ok(orderId);
    }


    @Transactional
    public void createVocherOrder(VoucherOrder voucherOrder){
        //6.一人一单
        Long userId = voucherOrder.getUserId();

        //6.1 查询订单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        //6.2判断订单是否存在， 是否购买过
        if(count > 0){
            log.error("用户已经购买过一次");
            return;
        }

        //5.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                .update();
        if(!success){
            //扣减失败
            log.error("库存不足！");
            return;
        }


        /*//6.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //6.1 订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //6.2 用户id
        //Long userId = UserHolder.getUser().getId();
        voucherOrder.setUserId(userId);
        //6.3 代金卷id
        voucherOrder.setVoucherId(voucherOrder);*/
        save(voucherOrder);
    }

    /*@Override
    public Result seckillVocher(Long voucherId) {
        //1.查询优惠卷id
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //2.判断秒杀是否开始
        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
            //如果尚未开始
            return Result.fail("秒杀尚未开始");
        }
        //3.判断秒杀是否结束
        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
            return Result.fail("秒杀已经结束");
        }
        //4.判断库存是否充足
        if(voucher.getStock() < 1){
            //库存不足， 直接返回报错
            return Result.fail("库存不足");
        };

        Long userId = UserHolder.getUser().getId();
        synchronized (userId.toString().intern()){
            //IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            //return proxy.createVocherOrder(voucherId);
            return  voucherOrderService.createVocherOrder(voucherId);
        }

        //创建锁对象
        //SimpleRedisLock redisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);

        //用redisson 创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        //获取锁
        //boolean isLock = redisLock.tryLock(1200);

        boolean isLock = lock.tryLock();
        //判断释放获取成功
        if(!isLock){
            //获取锁失败，返回错误或重试
            return Result.fail("无法重复下单");
        }
        try {
            //获取代理对象
            return voucherOrderService.createVocherOrder(voucherId);
        } finally {
            //释放锁
            lock.unlock();
        }
    }*/
}
