package com.flink.demo.queryable.util;

/**
 * @author: xzp
 * @date: 2018-06-06
 * @description: Storm流中数据字典的键
 * <p>
 * 数字下标的采用 _索引数字_易读命名 的格式
 * 最小有效值: MIN_索引数字_易读命名
 * 最大有效值: MAX_索引数字_易读命名
 */
@SuppressWarnings({"AlibabaAvoidStartWithDollarAndUnderLineNaming", "unused"})
public final class DataKey {

    /**
     * 消息前缀
     */
    public static final String PREFIX = "PREFIX";

    /**
     * 消息序号
     */
    public static final String SERIAL_NO = "SERIAL_NO";

    /**
     * VIN
     */
    public static final String VEHICLE_NUMBER = "VIN";

    /**
     * 消息类型
     */
    public static final String MESSAGE_TYPE = "MESSAGETYPE";

    /**
     * 车辆ID, 车辆在平台中唯一编码
     */
    public static final String VEHICLE_ID = "VID";

    /**
     * 时间
     */
    public static final String TIME = "TIME";

    /**
     * 车辆类型, 预警用于匹配约束条件
     */
    public static final String VEHICLE_TYPE = "VTYPE";

    /**
     * 车机类型, 接入标识_协议种类_协议版本, 可通过CommandUtils工具类解析.
     * 接入标识:1.前置机接入 2.平台转发接入
     * 协议种类:1.国标协议 2.地标协议
     * 协议版本:1.0.0
     */
    public static final String CAR_TYPE = "CTYPE";

    /**
     * 采集时间, 国标, 这个值可能超过范围, 有效判定条件是于服务器接收时间(9999)相差10分钟以内.
     */
    public static final String _2000_TERMINAL_COLLECT_TIME = "2000";

    /**
     * 单体蓄电池总数
     */
    public static final String _2001 = "2001";

    /**
     * 动力蓄电池包总数
     */
    public static final String _2002 = "2002";

    /**
     * 单体蓄电池电压值列表
     */
    public static final String _2003_SINGLE_VOLT = "2003";

    /**
     * 蓄电池包温度探针总数
     */
    public static final String _2101 = "2101";

    /**
     * 蓄电池包总数
     */
    public static final String _2102 = "2102";

    /**
     * 单体温度值列表
     */
    public static final String _2103_SINGLE_TEMP = "2103";

    /**
     * 车速
     */
    public static final String _2201_SPEED = "2201";

    /**
     *
     */
    public static final String _2110 = "2110";

    /**
     *
     */
    public static final String _2111 = "2111";

    /**
     *
     */
    public static final String _2114 = "2114";

    /**
     *
     */
    public static final String _2115 = "2115";

    /**
     *
     */
    public static final String _2119 = "2119";

    /**
     * 车速最小有效值
     * #国标
     */
    public static final short MIN_2201_SPEED = 0;

    /**
     * 车速最大有效值
     * #国标
     */
    public static final short MAX_2201_SPEED = 2200;

    /**
     * 累计里程, 总里程
     * #国标
     */
    public static final String _2202_TOTAL_MILEAGE = "2202";
    public static final String CACHE_2202_TOTAL_MILEAGE_MIN = "min(2202)";
    public static final String CACHE_2202_TOTAL_MILEAGE_MAX = "max(2202)";

    /**
     * 累计里程最小有效值
     * #国标
     */
    public static final int MIN_2202_TOTAL_MILEAGE = 0;

    /**
     * 累计里程最大有效值
     * #国标
     */
    public static final int MAX_2202_TOTAL_MILEAGE = 9999999;

    /**
     * GEARS 档位
     */
    public static final String _2203_GEARS = "2203";

    /**
     * 制动状态
     */
    public static final String _2204_BRAKING_FORCE = "2204";

    /**
     * 驱动状态
     */
    public static final String _2205_DRIVING_FORCE = "2205";

    /**
     * 加速踏板行程值
     */
    public static final String _2208_ACCELERATOR_PEDAL = "2208";

    /**
     * 制动踏板行程值
     */
    public static final String _2209_BRAKING_PEDAL = "2209";

    /**
     * 空调设定温度
     */
    public static final String _2210 = "2210";

    /**
     * SOC 过高告警
     */
    public static final String _2213_RUNNING_MODE = "2213";

    /**
     * DC-DC状态
     */
    public static final String _2214 = "2214";

    /**
     * 续驶里程
     */
    public static final String _2290 = "2290";

    /**
     * 充放电状态, 1-停车充电, 2-行驶充电, 3-未充电状态, 4-充电完成
     */
    public static final String _2301_CHARGE_STATUS = "2301";

    /**
     * 停车充电状态
     */
    public static final String _2301_CHARGE_STATUS_CHARGING = "1";

    /**
     * 电机控制器温度
     */
    public static final String _2302_DRIVING_ELE_MAC_TEMPCTOL = "2302";

    /**
     * 驱动电机转速
     */
    public static final String _2303_DRIVING_ELE_MAC_REV = "2303";

    /**
     * 驱动电机温度
     */
    public static final String _2304_DRIVING_ELE_MAC_TEMP = "2304";

    /**
     * 驱动电机输入电压
     */
    public static final String _2305_DRIVING_ELE_MAC_VOLT = "2305";

    /**
     * 驱动电机母线电流
     */
    public static final String _2306_DRIVING_ELE_MAC_ELE = "2306";

    /**
     * 驱动电机个数
     */
    public static final String _2307_DRIVING_ELE_MAC_COUNT = "2307";

    /**
     * 驱动电机列表
     */
    public static final String _2308_DRIVING_ELE_MAC_LIST = "2308";

    /**
     * 驱动电机序号
     */
    public static final String _2309_DRIVING_ELE_MAC_SEQ = "2309";

    /**
     * 驱动电机状态
     */
    public static final String _2310_DRIVING_ELE_MAC_STATUS = "2310";

    /**
     * 驱动电机转矩
     */
    public static final String _2311_DRIVING_ELE_MAC_TORQUE = "2311";

    /**
     * 发动机状态
     */
    public static final String _2401_ENGINES = "2401";

    /**
     * ECU温度
     */
    public static final String _2402 = "2402";

    /**
     * 车辆电池电压
     */
    public static final String _2403 = "2403";

    /**
     * 发动机温度
     */
    public static final String _2404 = "2404";

    /**
     * 进气歧管气压
     */
    public static final String _2405 = "2405";

    /**
     * 进气温度
     */
    public static final String _2406 = "2406";

    /**
     * 废气排出温度
     */
    public static final String _2407 = "2407";

    /**
     * 燃料喷射压力
     */
    public static final String _2408 = "2408";

    /**
     * 燃料喷射量
     */
    public static final String _2409 = "2409";

    /**
     * 点火提前角
     */
    public static final String _2410 = "2410";

    /**
     * 曲轴转速
     */
    public static final String _2411 = "2411";

    /**
     * 油门开度
     */
    public static final String _2412 = "2412";

    /**
     *
     */
    public static final String _2413 = "2413";

    /**
     * 定位状态
     * bit[0]: 0-有效, 1-无效, 无效时可能收到失效前最后的有效值.
     * bit[1]: 0-北纬, 1-南纬
     * bit[2]: 0-东经, 1-西经
     * bit[3-7]: 保留
     */
    public static final String _2501_ORIENTATION = "2501";

    /**
     * 有效位掩码
     */
    public static final int ORIENTATION_MASK_QUALITY = 0x00000001;

    /**
     * 有效定位
     */
    public static final int ORIENTATION_MASK_QUALITY_USEFUL = 0x00000000;

    /**
     * 无效定位
     */
    public static final int ORIENTATION_MASK_QUALITY_USELESS = 0x00000001;

    /**
     * 经度位掩码
     */
    public static final int ORIENTATION_MASK_LONGITUDE = 0x00000004;

    /**
     * 东经
     */
    public static final int ORIENTATION_MASK_LONGITUDE_EAST = 0x00000000;

    /**
     * 西经
     */
    public static final int ORIENTATION_MASK_LONGITUDE_WEST = 0x00000004;

    /**
     * 纬度位掩码
     */
    public static final int ORIENTATION_MASK_LATITUDE = 0x00000002;

    /**
     * 北纬
     */
    public static final int ORIENTATION_MASK_LATITUDE_NORTH = 0x00000000;

    /**
     * 南纬
     */
    public static final int ORIENTATION_MASK_LATITUDE_SOUTH = 0x00000002;

    /**
     * 经纬度精度, 精确到小数点后6位.
     */
    public static final double ORIENTATION_PRECISION = 1000000D;

    /**
     * 经度
     */
    public static final String _2502_LONGITUDE = "2502";

    /**
     * 经度最小有效值, 0°, 精确到小数点后6位.
     */
    public static final int MIN_2502_LONGITUDE = 0;

    /**
     * 经度最大有效值, 180°, 精确到小数点后6位.
     */
    public static final int MAX_2502_LONGITUDE = 180000000;

    /**
     * 纬度
     */
    public static final String _2503_LATITUDE = "2503";

    /**
     * 纬度最小有效值, 0°, 精确到小数点后6位.
     */
    public static final int MIN_2503_LATITUDE = 0;

    /**
     * 纬度最大有效值, 90°, 精确到小数点后6位.
     */
    public static final int MAX_2503_LATITUDE = 90000000;

    /**
     * 速度
     */
    public static final String _2504 = "2504";

    /**
     * 方向
     */
    public static final String _2505 = "2505";

    /**
     * 最高电压动力蓄电池单体所在电池包序号, 最高电压电池子系统号
     */
    public static final String _2601_HIGHVOLT_CHILD_NUM = "2601";

    /**
     * 最高电压单体蓄电池序号, 最高电压电池单体代号
     */
    public static final String _2602_HIGHVOLT_SINGLE_NUM = "2602";

    /**
     * 电池单体电压最高值
     */
    public static final String _2603_SINGLE_VOLT_HIGN_VAL = "2603";

    /**
     * 最低电压动力蓄电池包序号, 最低电压电池子系统号
     */
    public static final String _2604_LOWVOLT_CHILD_NUM = "2604";

    /**
     * 最低电压单体蓄电池序号, 最低电压电池单体代号
     */
    public static final String _2605_LOWVOLT_SINGLE_NUM = "2605";

    /**
     * 电池单体电压最低值
     */
    public static final String _2606_SINGLE_VOLT_LOW_VAL = "2606";

    /**
     * 最高温度子系统号
     */
    public static final String _2607_HIGNTEMP_CHILD = "2607";

    /**
     * 最高温度探针单体代号
     */
    public static final String _2608_SINGLE_HIGNTEMP_NUM = "2608";

    /**
     * 电池单体最高温度值
     */
    public static final String _2609_SINGLE_HIGNTEMP_VAL = "2609";

    /**
     * 最低温度子系统号
     */
    public static final String _2610_LOWTEMP_CHILD = "2610";

    /**
     * 最低温度探针单体代号
     */
    public static final String _2611_SINGLE_LOWTEMP_NUM = "2611";

    /**
     * 电池单体最低温度值
     */
    public static final String _2612_SINGLE_LOWTEMP_VAL = "2612";

    /**
     * 总电压
     */
    public static final String _2613_TOTAL_VOLTAGE = "2613";

    /**
     * 总电压最小有效值
     */
    public static final short MIN_2613_TOTAL_VOLTAGE = 0;

    /**
     * 总电压最大有效值
     */
    public static final short MAX_2613_TOTAL_VOLTAGE = 10000;

    /**
     * 总电流
     */
    public static final String _2614_TOTAL_ELECTRICITY = "2614";

    /**
     * 总电流最小有效值
     */
    public static final short MIN_2614_TOTAL_ELECTRICITY = 0;

    /**
     * 总电流最大有效值
     */
    public static final short MAX_2614_TOTAL_ELECTRICITY = 20000;

    /**
     * SOC 电池剩余电量百分比, 已弃用
     * State of Charge, 荷电状态
     * #地标, 步进0.4%
     */
    @Deprecated
    public static final String _2615_STATE_OF_CHARGE_BEI_JIN = "2615";

    /**
     *
     */
    public static final short MIN_2615_STATE_OF_CHARGE_BEI_JIN = 0;

    /**
     *
     */
    public static final short MAX_2615_STATE_OF_CHARGE_BEI_JIN = 250;

    /**
     * 剩余能量
     */
    public static final String _2616 = "2616";

    /**
     * 绝缘电阻
     */
    public static final String _2617_INSULATION_RESISTANCE = "2617";

    /**
     * 自定义类型编码列表
     */
    public static final String _2701 = "2701";

    /**
     * 自定义类型数据列表
     */
    public static final String _2702 = "2702";

    /**
     * 动力蓄电池报警标志
     */
    public static final String _2801_POWER_BATTERY_ALARM_FLAG_2801 = "2801";

    /**
     * 动力蓄电池其他故障总数
     */
    public static final String _2802 = "2802";

    /**
     * 动力蓄电池其他故障代码列表
     */
    public static final String _2803 = "2803";

    /**
     * 电机故障总数
     */
    public static final String _2804 = "2804";

    /**
     * 驱动电机故障代码列表
     */
    public static final String _2805 = "2805";

    /**
     * 发动机故障总数
     */
    public static final String _2806 = "2806";

    /**
     * 发动机故障列表
     */
    public static final String _2807 = "2807";

    /**
     * 其他故障总数
     */
    public static final String _2808 = "2808";

    /**
     * 其他故障代码列表
     */
    public static final String _2809 = "2809";

    /**
     * SOC 过高告警
     */
    public static final String _2909_SOC_HIGH_ALARM = "2909";

    /**
     * 最高报警等级
     * 为当前发生的故障中的最高等级值，有效值范围：0～3
     * “0”表示无故障
     * “1”表示 1 级故障，指代不影响车辆正常行驶的故障
     * “2”表示 2 级故障，指代影响车辆性能，需驾驶员限制行驶的故障
     * “3”表示 3 级故障，为最高级别故障，指代驾驶员应立即停车处理或请求救援的故障；
     * 具体等级对应的故障内容由厂商自行定义, “0xFE”表示异常, “0xFF”表示无效
     */
    public static final String _2920_ALARM_STATUS = "2920";

    public static final String _2921 = "2921";

    /**
     * 可充电储能装置故障代码列表
     */
    public static final String _2922 = "2922";

    public static final String _2923 = "2923";

    /**
     * 发动机故障代码列表
     */
    public static final String _2924 = "2924";

    /**
     * 采集时间, 地标.
     */
    public static final String _3101_COLLECT_TIME = "3101";

    /**
     * 状态标志
     */
    public static final String _3110_STATUS_FLAGS = "3110";

    /**
     * 车辆状态, 1-车辆启动状态, 2-熄火, 3-其他状态
     */
    public static final String _3201_CAR_STATUS = "3201";

    /**
     * 通用报警标志值
     */
    public static final String _3801_ALARM_MARK = "3801";

    public static final String _7001 = "7001";

    public static final String _7002 = "7002";

    /**
     * 单体电压原始报文
     */
    public static final String _7003_SINGLE_VOLT_ORIG = "7003";

    public static final String _7101 = "7101";

    public static final String _7102 = "7102";

    /**
     * 单体温度原始报文
     */
    public static final String _7103_SINGLE_TEMP_ORGI = "7103";

    /**
     * SOC 电池剩余电量百分比
     * State of Charge, 荷电状态
     * #国标, 步进1%
     */
    public static final String _7615_STATE_OF_CHARGE = "7615";
    public static final byte MIN_7615_STATE_OF_CHARGE = 0;
    public static final byte MAX_7615_STATE_OF_CHARGE = 100;

    /**
     * SERVER_TIME服务器接收到报文的时间
     */
    public static final String _9999_PLATFORM_RECEIVE_TIME = "9999";

    /**
     * 车辆是否在线, 0-离线, 1-在线
     */
    public static final String _10002_IS_ONLINE = "10002";

    /**
     * can 列表
     */
    public static final String _4410023_CAN_LIST = "4410023";

    // region 吉利

    // region 终端锁车状态

    /**
     * 锁车功能状态, 0-锁车功能关闭, 1-锁车功能打开.
     */
    public static final String _4710061_JILI_LOCK_FUNCTION = "4710061";
    public static final String _4710061_JILI_LOCK_FUNCTION_DISABLE = "0";
    public static final String _4710061_JILI_LOCK_FUNCTION_ENABLE = "1";

    /**
     * 锁车状态, 0-车辆未被锁定, 1-车辆被锁定.
     */
    public static final String _4710062_JILI_LOCK_STATUS = "4710062";
    /**
     * 动力未锁止
     */
    public static final String _4710062_JILI_LOCK_STATUS_UNLOCK = "0";
    /**
     * 动力锁止
     */
    public static final String _4710062_JILI_LOCK_STATUS_LOCKED = "1";
    /**
     * 禁止锁止动力
     */
    public static final String _4710062_JILI_LOCK_STATUS_DISABLE = "2";

    // endregion 终端锁车状态

    // endregion 吉利
}
