package com.test.util;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.util.RMHAUtils;
import org.json.JSONArray;
import org.json.JSONObject;

public class YarnUtil {
	CMUtil cmUtil = new CMUtil();

	/**
	 * 检测是否存在队列
	 * 
	 * @param schedulerinfo
	 *            通过调用yarn rest api获得的调度器信息，获得是json格式，转为xml再解析
	 * @param queueName
	 *            队列名称
	 * @return 返回是否存在调度器
	 * @throws Exception
	 */
	public boolean existQueue(String schedulerinfo, String queueName)
			throws Exception {
		boolean existQueue = false;
		JSONObject schedulerinfo_json = new JSONObject(schedulerinfo);
		JSONArray queues_json = schedulerinfo_json.getJSONObject("scheduler")
				.getJSONObject("schedulerInfo").getJSONObject("queues")
				.getJSONArray("queue");
		for (int i = 0; i < queues_json.length(); i++) {
			JSONObject queue_json = queues_json.getJSONObject(i);
			if (!queueName.equals(queue_json.getString("queueName"))) {
				continue;
			}
			existQueue = true;
			break;
		}
		return existQueue;
	}

	/**
	 * 获得application的状态
	 * 
	 * @param appid
	 *            application id
	 * @return 返回状态
	 * @throws Exception
	 */
	public String getAppStatus(String appid) throws Exception {
		YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(new Configuration());
		yarnClient.start();
		long cluster = Long.parseLong(appid.substring(12, 25));
		int id = Integer.parseInt(appid.substring(27));
		ApplicationId applicationid = ApplicationId.newInstance(cluster, id);
		ApplicationReport report = yarnClient
				.getApplicationReport(applicationid);
		String status = report.getFinalApplicationStatus().toString();
		yarnClient.close();
		return status;
	}

	/**
	 * 获得执行application的container日志信息
	 * 
	 * @param appid
	 *            application id
	 * @param appLocalLogPath
	 *            日志输出路径
	 * @throws Exception
	 */
	public void getContainersLogs(String appid, String appLocalLogPath)
			throws Exception {
		Configuration config = new YarnConfiguration();
		Path remoteRootLogDir = null;
		// Get the value of the name property as an int. If no such property
		// exists, the provided default value is returned
		remoteRootLogDir = new Path(config.get(
				"yarn.nodemanager.remote-app-log-dir", "/tmp/logs"));
		YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(config);
		yarnClient.start();
		long cluster = Long.parseLong(appid.substring(12, 25));
		int id = Integer.parseInt(appid.substring(27));
		ApplicationId applicationid = ApplicationId.newInstance(cluster, id);
		String user = yarnClient.getApplicationReport(applicationid).getUser();
		String logDirSuffix = LogAggregationUtils
				.getRemoteNodeLogDirSuffix(config);

		org.apache.hadoop.fs.Path remoteAppLogDir = LogAggregationUtils
				.getRemoteAppLogDir(remoteRootLogDir, applicationid, user,
						logDirSuffix);
		RemoteIterator<?> nodeFiles;
		try {
			org.apache.hadoop.fs.Path qualifiedLogDir = FileContext
					.getFileContext(config).makeQualified(remoteAppLogDir);
			nodeFiles = FileContext.getFileContext(qualifiedLogDir.toUri(),
					config).listStatus(remoteAppLogDir);
		} catch (FileNotFoundException fnf) {
			JavaUtil.log("Logs not available at " + remoteAppLogDir.toString(),
					appLocalLogPath);
			JavaUtil.log(
					"Log aggregation has not completed or is not enabled.",
					appLocalLogPath);
			return;
		}

		while (nodeFiles.hasNext()) {
			FileStatus thisNodeFile = (FileStatus) nodeFiles.next();
			AggregatedLogFormat.LogReader reader = new AggregatedLogFormat.LogReader(
					config, new org.apache.hadoop.fs.Path(remoteAppLogDir,
							thisNodeFile.getPath().getName()));
			try {
				AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();
				DataInputStream valueStream = reader.next(key);
				while (valueStream != null) {
					String containerString = "\n\nContainer: " + key + " on "
							+ thisNodeFile.getPath().getName();

					JavaUtil.log(containerString, appLocalLogPath);
					JavaUtil.log(
							StringUtils.repeat("=", containerString.length()),
							appLocalLogPath);
					try {
						while (true) {
							readAContainerLogsForALogType(valueStream,
									appLocalLogPath);
						}
					} catch (EOFException eof) {
						key = new AggregatedLogFormat.LogKey();
						valueStream = reader.next(key);
					}
				}
			} finally {
				reader.close();
			}
		}
	}

	/**
	 * 获得队列中完成的application数量
	 * 
	 * @param queueName
	 *            队列名称
	 * @return 返回完成数量
	 * @throws Exception
	 */
	public int getNumFinishedApps(String queueName) throws Exception {
		YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(new Configuration());
		yarnClient.start();
		List<ApplicationReport> appslist = yarnClient.getApplications();
		int num = 0;
		for (int i = 0; i < appslist.size(); i++) {
			ApplicationReport application = appslist.get(i);
			if (queueName.equals(application.getQueue())
					&& "FINISHED".equals(application.getYarnApplicationState()
							.toString())) {
				num++;
			}
		}
		yarnClient.close();
		return num;
	}

	/**
	 * 获得ResourceManager的active id
	 * 
	 * @return
	 */
	public String getRMHAId() {
		YarnConfiguration conf = new YarnConfiguration();
		String rmid = RMHAUtils.findActiveRMHAId(conf);
		return "rmid>>>>>>" + rmid;
	}

	/**
	 * 获得ResourceManager的active webAppAddr
	 * 
	 * @return
	 */
	public String getRmWebappAddr() {
		YarnConfiguration conf = new YarnConfiguration();
		boolean isHA = HAUtil.isHAEnabled(conf);
		String rmAddr = "";
		if (isHA) {
			String rmid = RMHAUtils.findActiveRMHAId(conf);
			rmAddr = conf.get("yarn.resourcemanager.webapp.address." + rmid);
		} else {
			rmAddr = conf.get("yarn.resourcemanager.webapp.address");
		}
		return rmAddr;
	}

	/**
	 * 检测队列是否改变
	 * 
	 * @param queueName
	 *            队列名称
	 * @param changeName
	 *            队列中改变的属性名称
	 * @param changeValue
	 *            队列中改变的属性值
	 * @return 返回是否改变
	 * @throws Exception
	 */
	public boolean isQueueChanged(String queueName, String changeName,
			double changeValue) throws Exception {
		boolean isQueueChanged = false;
		String rmWebAppAddr = getRmWebappAddr(); // 192.168.8.203:8088
		String schedulerInfo = JavaUtil.getMethod("http://" + rmWebAppAddr
				+ "/ws/v1/cluster/scheduler");
		JSONArray queues_ja = (new JSONObject(schedulerInfo))
				.getJSONObject("scheduler").getJSONObject("schedulerInfo")
				.getJSONObject("queues").getJSONArray("queue");
		for (int i = 0; i < queues_ja.length(); i++) {
			JSONObject queue_jo = queues_ja.getJSONObject(i);
			if (!queueName.equals(queue_jo.getString("queueName"))) {
				continue;
			}
			if (changeValue == queue_jo.getDouble(changeName)) {
				isQueueChanged = true;
			}
			break;
		}
		return isQueueChanged;
	}

	/**
	 * kill掉application
	 * 
	 * @param appid
	 *            要kill的application id
	 * @return
	 * @throws Exception
	 */
	public boolean killApp(String appid) throws Exception {
		YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(new Configuration());
		yarnClient.start();
		long cluster = Long.parseLong(appid.substring(12, 25));
		int id = Integer.parseInt(appid.substring(27));
		ApplicationId applicationid = ApplicationId.newInstance(cluster, id);
		yarnClient.killApplication(applicationid);

		String appStatus = getAppStatus(appid);
		if ("KILLED".equals(appStatus)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 解析containerLog的内容
	 * 
	 * @param valueStream
	 *            数据输入流
	 * @param appLogPath
	 *            日志输入路径
	 * @throws Exception
	 */
	public void readAContainerLogsForALogType(DataInputStream valueStream,
			String appLogPath) throws Exception {
		byte[] buf = new byte[65535];

		/**
		 * .readUTF(),首先读取两个字节，并使用它们构造一个无符号 16 位整数，构造方式与 readUnsignedShort
		 * 方法的方式完全相同。该整数值被称为 UTF长度，它指定要读取的额外字节数。
		 * 然后成组地将这些字节转换为字符。每组的长度根据该组第一个字节的值计算 紧跟在某个组后面的字节（如果有）是下一组的第一个字节。
		 */
		String fileType = valueStream.readUTF();
		String fileLengthStr = valueStream.readUTF();
		long fileLength = Long.parseLong(fileLengthStr);
		JavaUtil.log("LogType: ", appLogPath);
		JavaUtil.log(fileType, appLogPath);
		JavaUtil.log("LogLength: ", appLogPath);
		JavaUtil.log(fileLengthStr, appLogPath);
		JavaUtil.log("Log Contents:", appLogPath);

		long curRead = 0L;
		long pendingRead = fileLength - curRead;
		int toRead = pendingRead > buf.length ? buf.length : (int) pendingRead;

		int len = valueStream.read(buf, 0, toRead);
		OutputStream os = new FileOutputStream(appLogPath, true);
		while ((len != -1) && (curRead < fileLength)) {
			os.write(buf, 0, len);
			curRead += len;
			pendingRead = fileLength - curRead;
			toRead = pendingRead > buf.length ? buf.length : (int) pendingRead;
			len = valueStream.read(buf, 0, toRead);
		}
		os.close();
		JavaUtil.log("", appLogPath);
	}

}
