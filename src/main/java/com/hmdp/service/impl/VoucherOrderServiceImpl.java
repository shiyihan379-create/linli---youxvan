package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hmdp.utils.RedisConstants.*;

/**
 * 优惠券订单服务实现类
 * 核心业务：秒杀下单
 * 架构：Redis Lua原子预判 + Redis Stream消息队列异步下单 + 分布式锁 + 数据库事务
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    // 秒杀预判Lua脚本：原子性地完成库存判断、一人一单判断、扣减库存
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    // 分布式锁释放Lua脚本：保证只释放自己加的锁，防止误删别人的锁
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;

    // 静态代码块：在类加载时初始化两个Lua脚本
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setResultType(Long.class);
        SECKILL_SCRIPT.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/seckill.lua")));

        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setResultType(Long.class);
        UNLOCK_SCRIPT.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/unlock.lua")));
    }

    // 秒杀券服务，用于扣减数据库中的库存
    @Resource
    private ISeckillVoucherService seckillVoucherService;

    // Redis全局唯一ID生成器，用于生成订单ID
    @Resource
    private RedisIdWorker redisIdWorker;

    // Redis操作模板，用于执行Lua脚本、操作Stream消息队列、分布式锁等
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    // 当前Service的代理对象，用于调用createVoucherOrderWithTx()使@Transactional事务生效
    // @Lazy延迟加载，解决Service自己注入自己的循环依赖问题
    @Lazy
    @Resource
    private IVoucherOrderService voucherOrderService;

    // 单线程线程池，用于异步执行秒杀订单消费者
    // 单线程保证消息串行处理，避免并发问题
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    /**
     * Bean初始化方法，在Spring完成依赖注入后自动调用（@PostConstruct）
     * 做两件事：
     * 1. 确保Redis Stream消费者组存在
     * 2. 启动异步消费者线程，开始监听消息队列
     */
    @PostConstruct
    private void init() {
        // 确保消费者组存在，否则消费者读取消息时会报错
        createGroupIfNotExists();
        // 将VoucherOrderHandler提交到单线程池，开始异步消费订单消息
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    /**
     * 创建Redis Stream消费者组（如果不存在的话）
     * 对应Redis命令：XGROUP CREATE stream:order g1 $
     * 幂等操作：如果组已存在，Redis会抛异常，catch捕获即可
     */
    private void createGroupIfNotExists() {
        try {
            stringRedisTemplate.opsForStream().createGroup(SECKILL_STREAM_KEY, SECKILL_STREAM_GROUP);
            log.info("创建消费者组 {} 成功", SECKILL_STREAM_GROUP);
        } catch (Exception e) {
            // 消费者组已存在时Redis会抛BUSYGROUP异常，属于正常情况
            log.info("消费者组 {} 已存在", SECKILL_STREAM_GROUP);
        }
    }

    /**
     * 秒杀订单异步消费者（内部类）
     * 持续从Redis Stream消息队列中读取订单消息，执行数据库下单操作
     * 作为VoucherOrderServiceImpl的内部类，可以直接访问外部类的字段和方法
     */
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            // 死循环：消费者必须持续运行，一旦停止消息就无人处理
            while (true) {
                try {
                    // 以消费者组模式阻塞读取Redis Stream消息
                    // 对应Redis命令：XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream:order >
                    List<MapRecord<String, Object, Object>> messages = stringRedisTemplate.opsForStream().read(
                            Consumer.from(SECKILL_STREAM_GROUP, "c1"),    // 消费者组g1，消费者名c1
                            StreamReadOptions.empty().count(1)             // 每次最多读1条消息
                                    .block(Duration.ofSeconds(2)),         // 没有消息时阻塞等待2秒，避免空转消耗CPU
                            StreamOffset.create(SECKILL_STREAM_KEY,        // 从stream:order读取
                                    ReadOffset.lastConsumed())             // ">"表示只读尚未消费的新消息
                    );

                    // 阻塞超时或无消息时，跳过本轮继续等待
                    if (messages == null || messages.isEmpty()) {
                        continue;
                    }

                    // 取出第一条消息（count(1)保证最多只有1条）
                    MapRecord<String, Object, Object> message = messages.get(0);

                    // 将消息的Map结构解析为VoucherOrder对象
                    VoucherOrder voucherOrder = parseVoucherOrder(message.getValue());

                    // 解析失败（消息格式异常），ACK后跳过，避免坏消息导致死循环
                    if (voucherOrder == null) {
                        acknowledge(message.getId());
                        continue;
                    }

                    // 执行下单操作（分布式锁 + 事务）
                    try {
                        createVoucherOrder(voucherOrder);
                    } finally {
                        // 无论下单成功与否，都ACK确认消息
                        // 业务拒绝（一人一单、库存不足）重试也不会成功，直接丢弃
                        acknowledge(message.getId());
                    }
                } catch (Exception e) {
                    // 最外层异常兜底：捕获所有异常，保证while循环不会因异常而退出
                    // 如果循环退出，消费者线程死亡，订单消息将无人处理
                    log.error("处理订单异常", e);
                }
            }
        }
    }

    /**
     * 将Redis Stream消息的Map结构解析为VoucherOrder对象
     * 消息格式：{orderId: "xxx", userId: "xxx", voucherId: "xxx"}
     *
     * @param values 消息体Map
     * @return 解析成功返回VoucherOrder对象，解析失败返回null
     */
    private VoucherOrder parseVoucherOrder(Map<Object, Object> values) {
        try {
            VoucherOrder order = new VoucherOrder();
            order.setId(Long.valueOf(values.get("orderId").toString()));
            order.setUserId(Long.valueOf(values.get("userId").toString()));
            order.setVoucherId(Long.valueOf(values.get("voucherId").toString()));
            return order;
        } catch (Exception e) {
            log.error("解析订单信息异常", e);
            return null;
        }
    }

    /**
     * ACK确认消息，告诉Redis该消息已处理完毕
     * 对应Redis命令：XACK stream:order g1 <messageId>
     * ACK后消息从Pending列表移除，不会再被分配给消费者
     *
     * @param id 消息的RecordId
     */
    private void acknowledge(RecordId id) {
        stringRedisTemplate.opsForStream().acknowledge(SECKILL_STREAM_KEY, SECKILL_STREAM_GROUP, id);
    }

    /**
     * 秒杀下单入口方法（由Controller调用，主线程同步执行）
     * 核心思路：Redis做轻量预判 → 消息队列异步落库 → 快速返回
     *
     * @param voucherId 优惠券ID
     * @return 下单结果，成功返回订单ID
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 从ThreadLocal中获取当前登录用户ID
        Long userId = UserHolder.getUser().getId();

        // 1. 执行Lua脚本：原子性地完成以下4步操作（避免并发超卖和重复下单）
        //    ① 判断库存是否充足
        //    ② 判断用户是否已下过单（一人一单）
        //    ③ 扣减Redis中的库存
        //    ④ 记录用户已下单
        //    返回值：0=库存不足，1=已下过单，2=预判成功
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Arrays.asList(SECKILL_STOCK_KEY + voucherId, SECKILL_ORDER_KEY + voucherId),
                userId.toString()
        );
        int r = result.intValue();
        if (r != 2) {
            // Lua脚本返回非2，秒杀预判失败，直接返回错误
            return Result.fail(r == 0 ? "库存不足" : "每人限购一单");
        }

        // 2. Lua脚本预判成功，构造订单对象
        VoucherOrder voucherOrder = new VoucherOrder();
        // 使用Redis全局唯一ID生成器生成订单ID（雪花算法）
        Long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);

        // 将订单信息封装为Map，发送到Redis Stream消息队列
        // 生产者：往队列放消息，消费者（VoucherOrderHandler）会异步取出处理
        Map<String, Object> message = new HashMap<>(3);
        message.put("orderId", orderId.toString());
        message.put("userId", userId.toString());
        message.put("voucherId", voucherId.toString());
        stringRedisTemplate.opsForStream().add(SECKILL_STREAM_KEY, message);

        // 3. 异步下单：主线程不等待数据库操作完成，直接返回订单ID给前端
        return Result.ok(orderId);
    }

    /**
     * 带分布式锁的下单方法（由异步消费者VoucherOrderHandler调用）
     * 使用Redis分布式锁保证同一用户同一优惠券不会并发重复下单
     * 锁粒度：userId + voucherId，不同用户/不同优惠券互不影响
     *
     * @param voucherOrder 订单信息
     */
    private void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        // 构造分布式锁的Key：lock:order:{userId}:{voucherId}
        // 锁Value使用UUID，用于安全释放锁时验证是否是自己的锁
        String lockKey = LOCK_ORDER_KEY + userId + ":" + voucherId;
        String lockValue = UUID.randomUUID().toString();

        // 尝试获取分布式锁：SETNX + 过期时间（SET lockKey lockValue EX 10 NX）
        // 如果锁已存在（其他线程持有），获取失败
        Boolean isLock = stringRedisTemplate.opsForValue()
                .setIfAbsent(lockKey, lockValue, LOCK_ORDER_TTL, java.util.concurrent.TimeUnit.SECONDS);
        if (!Boolean.TRUE.equals(isLock)) {
            log.error("获取分布式锁失败，userId={}, voucherId={}", userId, voucherId);
            return;
        }
        try {
            // 获取锁成功，通过代理对象调用事务方法
            // 必须通过voucherOrderService调用，而不是this，否则@Transactional事务不生效
            voucherOrderService.createVoucherOrderWithTx(voucherOrder);
        } finally {
            // 释放分布式锁：使用Lua脚本保证"判断锁值+删除锁"的原子性
            // 防止误删别人的锁：先判断锁的值是否等于自己的UUID，是才删除
            stringRedisTemplate.execute(UNLOCK_SCRIPT,
                    Collections.singletonList(lockKey),
                    lockValue);
        }
    }

    /**
     * 事务下单方法：在数据库事务中完成最终下单
     * 包含三步操作：一人一单校验 → 扣减库存 → 保存订单
     * 任何一步失败，整个事务回滚
     *
     * 注意：必须通过Spring代理对象调用（voucherOrderService.createVoucherOrderWithTx），
     * 而不是this直接调用，否则@Transactional注解不生效（AOP代理问题）
     *
     * @param voucherOrder 订单信息
     */
    @Transactional
    public void createVoucherOrderWithTx(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        // 1. 数据库层面二次校验一人一单（Redis预判可能有延迟，数据库校验是最终兜底）
        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            log.error("一人一单校验失败，userId={}, voucherId={}", userId, voucherId);
            return;
        }

        // 2. 扣减数据库库存（乐观锁方式：WHERE stock > 0，防止超卖）
        // 对应SQL：UPDATE seckill_voucher SET stock = stock - 1 WHERE voucher_id = ? AND stock > 0
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
        if (!success) {
            log.error("扣减库存失败，voucherId={}", voucherId);
            return;
        }

        // 3. 保存订单到数据库
        save(voucherOrder);
    }
}