package com.test.util;

public class InitConfig {
	
	//httpservlet 实现了serializable 接口。所以这里的id的作用就是用来标识这个对象写入文件后的标识。
	
	public static final String BASEPATH = ConfigContext.getInstance().getString("basePath"); // 算子父目录
	public static final String STDERR = ConfigContext.getInstance().getString("stderr"); // 标准输出信息保存文件路径
	public static final String STDOUT = ConfigContext.getInstance().getString("stdout"); // 标准错误信息保存文件路径
	public static final String APPID =  ConfigContext.getInstance().getString("appid"); //applicatonid信息保存文件路径
	public static final String LOCALAPPLOGS =  ConfigContext.getInstance().getString("localAppLogs"); //application的日志文件本地保存地址
	public static final String HDFSAPPLOGS =  ConfigContext.getInstance().getString("hdfsAppLogs"); //application的日志文件hdfs保存地址
	//public static final String OPSERVICELOGS =  ConfigContext.getInstance().getString("opServieLogs"); //相关
	public static final String REQUESTFILEPATH = ConfigContext.getInstance().getString("requestFilePath"); // 接收的xml参数保存文件路径
	public static final String REQUESTFILEPATH_OTHER = ConfigContext.getInstance().getString("requestFilePathOther"); // 接收的xml参数保存文件路径
	public static final String CRON_DELETEOPTEMP = ConfigContext.getInstance().getString("cronDeleteOpTemp"); //删除算子包缓存定时任务表达式
	public static final String CRON_DELETELOG = ConfigContext.getInstance().getString("cronDeleteLog"); //删除日志文件定时任务表达式
	public static final int DELETELOG_DAY = ConfigContext.getInstance().getInteger("deleteLogDay"); //删除多少天之前的日志文件
	public static final String CHECKSUM_DIR = ConfigContext.getInstance().getString("checksumDir"); //校验码目录名

	public static final String URLMYSQL = ConfigContext.getInstance().getString("mysql.url"); //mysql地址
	public static final String USERMYSQL = ConfigContext.getInstance().getString("mysql.user"); //用户名
	public static final String PASSMYSQL = ConfigContext.getInstance().getString("mysql.psw"); //密码
	
	public static final String URLHIVEMETADATA = ConfigContext.getInstance().getString("hiveMetaData.url"); //hive元数据库地址
	public static final String USERHIVEMETADATA = ConfigContext.getInstance().getString("hiveMetaData.user"); //用户名
	public static final String PASSHIVEMETADATA = ConfigContext.getInstance().getString("hiveMetaData.psw"); //密码
	
	public static final String CM_ADDR = ConfigContext.getInstance().getString("cm.url"); //cm界面地址
	public static final String CM_USER = ConfigContext.getInstance().getString("cm.user"); //用户名
	public static final String CM_PSW = ConfigContext.getInstance().getString("cm.psw"); //密码

	
	public static final String MAXQUOTA = ConfigContext.getInstance().getString("maxQuota"); // 最大配额 50GB*1073741824
	public static final String ROOT_PATH = ConfigContext.getInstance().getString("rootPath"); // 存储资源hdfs根目录

	public static final String FUNCTIONJARPATH = ConfigContext.getInstance().getString("functionJarPath"); //udf包在hdfs的保存路径
	

}
