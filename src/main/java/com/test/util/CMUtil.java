package com.test.util;


import java.util.HashMap;
import java.util.Map;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.json.JSONArray;
import org.json.JSONObject;

public class CMUtil {

	/**
	 * 计算所有nodemanager的总CPU数量
	 * 
	 * @return 返回CPU数量
	 * @throws Exception
	 */
	public int calculateTotalCpu() throws Exception {
		// String yarnRoles =
		// getMethod("http://192.168.8.206:7180/api/v9/clusters/cluster/services/yarn/roles");
		String yarnRoles = getMethod(InitConfig.CM_ADDR + "/api/v9/clusters/"
				+ getClusterName() + "/services/" + getYarnServiceName()
				+ "/roles");
		Map<String, Object> NodeManagerNumMap = parseNodeManagerNum(yarnRoles);
		System.out.println("NodeManagerNumMap>>>>>>>>>>>>>>>>>>");
		System.out.println(NodeManagerNumMap);
		// String roleConfigGroups =
		// getMethod("http://192.168.8.206:7180/api/v9/clusters/cluster/services/yarn/roleConfigGroups");
		String roleConfigGroups = getMethod(InitConfig.CM_ADDR
				+ "/api/v9/clusters/" + getClusterName() + "/services/"
				+ getYarnServiceName() + "/roleConfigGroups");
		System.out.println("roleConfigGroups>>>>>>>>>");
		System.out.println(roleConfigGroups);
		int totalCpu = 0;
		JSONArray roleConfigGroups_ja = (new JSONObject(roleConfigGroups))
				.getJSONArray("items");
		for (int i = 0; i < roleConfigGroups_ja.length(); i++) {
			JSONObject roleConfigGroup_jo = roleConfigGroups_ja
					.getJSONObject(i);
			if (!"NODEMANAGER".equals(roleConfigGroup_jo.getString("roleType"))) {
				continue;
			}
			JSONArray config_ja = roleConfigGroup_jo.getJSONObject("config")
					.getJSONArray("items");
			int NodeManagerGroupNum = (int) NodeManagerNumMap
					.get(roleConfigGroup_jo.getString("name"));
			int nodeManagerGroupCpu = 0;
			for (int j = 0; j < config_ja.length(); j++) {
				JSONObject item_jo = config_ja.getJSONObject(j);
				if ("yarn_nodemanager_resource_cpu_vcores".equals(item_jo
						.getString("name"))) {
					nodeManagerGroupCpu = item_jo.getInt("value");
					break;
				}
			}
			totalCpu += nodeManagerGroupCpu * NodeManagerGroupNum;
		}
		return totalCpu;
	}

	/**
	 * 计算所有nodemanager的总内存大小
	 * 
	 * @return 返回总内存
	 * @throws Exception
	 */
	public int calculateTotalMemory() throws Exception {
		String yarnRoles = getMethod(InitConfig.CM_ADDR + "/api/v9/clusters/"
				+ getClusterName() + "/services/" + getYarnServiceName()
				+ "/roles");
		Map<String, Object> NodeManagerNumMap = parseNodeManagerNum(yarnRoles);
		String roleConfigGroups = getMethod(InitConfig.CM_ADDR
				+ "/api/v9/clusters/" + getClusterName() + "/services/"
				+ getYarnServiceName() + "/roleConfigGroups");

		int totalMemory = 0;
		JSONArray roleConfigGroups_ja = (new JSONObject(roleConfigGroups))
				.getJSONArray("items");
		for (int i = 0; i < roleConfigGroups_ja.length(); i++) {
			JSONObject roleConfigGroup_jo = roleConfigGroups_ja
					.getJSONObject(i);
			if (!"NODEMANAGER".equals(roleConfigGroup_jo.getString("roleType"))) {
				continue;
			}
			JSONArray config_ja = roleConfigGroup_jo.getJSONObject("config")
					.getJSONArray("items");
			int NodeManagerGroupNum = (int) NodeManagerNumMap
					.get(roleConfigGroup_jo.getString("name"));
			int nodeManagerGroupMemory = 0;
			for (int j = 0; j < config_ja.length(); j++) {
				JSONObject item_jo = config_ja.getJSONObject(j);
				if ("yarn_nodemanager_resource_memory_mb".equals(item_jo
						.getString("name"))) {
					nodeManagerGroupMemory = item_jo.getInt("value");
					break;
				}
			}
			totalMemory += nodeManagerGroupMemory * NodeManagerGroupNum;
		}
		return totalMemory;
	}

	/**
	 * 向cms（clouderManagerService）发送get请求
	 * 
	 * @param url
	 *            请求地址
	 * @return cms返回信息
	 * @throws Exception
	 */
	public String getMethod(String url) throws Exception {
		GetMethod getmethod = new GetMethod(url);
		HttpClient httpclient = new HttpClient();
		httpclient.getState().setCredentials(
				// 服务器认证
				AuthScope.ANY, // 认证域
				new UsernamePasswordCredentials(InitConfig.CM_USER,
						InitConfig.CM_PSW) // 用户名、密码认证
				);
		httpclient.executeMethod(getmethod);
		String s = getmethod.getResponseBodyAsString();
		s = new String(s.getBytes("ISO8859-1"), "UTF-8");
		// System.out.println(s);
		return s;
	}

	/**
	 * 获得集群总内存接口，用于测试，额外添加的，OpService中没有用到
	 * 
	 * @return 返回内存大小
	 * @throws Exception
	 */
	public String getTotalMemory() throws Exception {
		String memory = String.valueOf(calculateTotalMemory());
		return memory;
	}

	/**
	 * 获得集群总cpu数量接口，用于测试，额外添加的，OpService中没有用到
	 * 
	 * @return 返回cpu数量
	 * @throws Exception
	 */
	public String getTotalCpu() throws Exception {
		String cpu = String.valueOf(calculateTotalCpu());
		return cpu;
	}

	/**
	 * 获得集群名称接口,默认只有一个集群
	 * 
	 * @return 返回集群名称
	 * @throws Exception
	 */
	public String getClusterName() throws Exception {
		String cluster = getMethod(InitConfig.CM_ADDR + "/api/v9/clusters/");
		JSONObject jsonObject = new JSONObject(cluster);
		String clusterName = jsonObject.getJSONArray("items").getJSONObject(0)
				.getString("name");
		return clusterName;
	}

	/**
	 * 获得yarn服务名称
	 * 
	 * @return 返回服务名称
	 * @throws Exception
	 */
	public String getYarnServiceName() throws Exception {
		String cluster = getMethod(InitConfig.CM_ADDR + "/api/v9/clusters/"
				+ getClusterName() + "/services");
		JSONObject jsonObject = new JSONObject(cluster);
		JSONArray items_ja = jsonObject.getJSONArray("items");
		String yarnServiceName = "";
		for (int i = 0; i < items_ja.length(); i++) {
			JSONObject item_jo = items_ja.getJSONObject(i);
			if (!"YARN".equals(item_jo.getString("type"))) {
				continue;
			}
			yarnServiceName = item_jo.getString("name");
			break;
		}
		return yarnServiceName;
	}

	/**
	 * 获得resourceManager配置组名称
	 * 
	 * @return 返回角色(resourceManager)配置组配置信息
	 * @throws Exception
	 */
	public String getRMConfigGroupName() throws Exception {
		String roleConfigGroups = getMethod(InitConfig.CM_ADDR
				+ "/api/v9/clusters/" + getClusterName() + "/services/"
				+ getYarnServiceName() + "/roleConfigGroups");
		JSONObject jsonObject = new JSONObject(roleConfigGroups);
		JSONArray items_ja = jsonObject.getJSONArray("items");
		String RMConfigGroupName = "";
		for (int i = 0; i < items_ja.length(); i++) {
			JSONObject item_jo = items_ja.getJSONObject(i);
			if (!"RESOURCEMANAGER".equals(item_jo.getString("roleType"))) {
				continue;
			}
			RMConfigGroupName = item_jo.getString("name");
			break;
		}
		return RMConfigGroupName;
	}

	/**
	 * 获得resourceManager角色名称
	 * 
	 * @return
	 * @throws Exception
	 */
	public String getRMRoleName() throws Exception {
		String roles = getMethod(InitConfig.CM_ADDR + "/api/v9/clusters/"
				+ getClusterName() + "/services/" + getYarnServiceName()
				+ "/roles");
		JSONObject jsonObject = new JSONObject(roles);
		JSONArray items_ja = jsonObject.getJSONArray("items");
		String RMRoleName = "";
		for (int i = 0; i < items_ja.length(); i++) {
			JSONObject item_jo = items_ja.getJSONObject(i);
			if ("RESOURCEMANAGER".equals(item_jo.getString("type"))
					&& "UNKNOWN".equals(item_jo.getString("haStatus"))) {
				return "UNKNOWN";
			}
			if (!"RESOURCEMANAGER".equals(item_jo.getString("type"))
					|| !"ACTIVE".equals(item_jo.getString("haStatus"))) {
				continue;
			}
			RMRoleName = item_jo.getString("name");
			break;
		}
		return RMRoleName;
	}

	/**
	 * 计算集群中每个nodemanager group的数量
	 * 
	 * @param yarnroles
	 *            通过cms的接口获得的所有yarn角色的信息
	 * @return 返回存角色组名称与数量的map
	 * @throws Exception
	 */
	public Map<String, Object> parseNodeManagerNum(String yarnroles)
			throws Exception {
		int roleConfigGroupNum;
		Map<String, Object> roleConfigGroupMap = new HashMap<String, Object>();
		JSONArray yarnRoles_ja = (new JSONObject(yarnroles))
				.getJSONArray("items");
		for (int i = 0; i < yarnRoles_ja.length(); i++) {
			JSONObject yarnRole_jo = yarnRoles_ja.getJSONObject(i);
			if (!"NODEMANAGER".equals(yarnRole_jo.getString("type"))) {
				continue;
			}
			String roleConfigGroupName = yarnRole_jo.getJSONObject(
					"roleConfigGroupRef").getString("roleConfigGroupName");
			if (!roleConfigGroupMap.containsKey(roleConfigGroupName)) {
				roleConfigGroupMap.put(roleConfigGroupName, 1);
			} else {
				roleConfigGroupNum = (int) roleConfigGroupMap
						.get(roleConfigGroupName) + 1;
				roleConfigGroupMap.put(roleConfigGroupName, roleConfigGroupNum);
			}
		}
		return roleConfigGroupMap;
	}

	/**
	 * 向cms发送put请求
	 * 
	 * @param url
	 *            请求地址
	 * @param entity
	 *            请求体内容，
	 * @throws Exception
	 */
	public void putMethod(String url, String entity) throws Exception {
		// PutMethod putmethod = new PutMethod(InitConfig.groupConfigURL);
		PutMethod putmethod = new PutMethod(url);
		RequestEntity requestEntity = new ByteArrayRequestEntity(
				entity.getBytes());
		putmethod.setRequestEntity(requestEntity);
		// 执行请求
		HttpClient httpclient1 = new HttpClient();
		httpclient1.getState().setCredentials(
				// 服务器认证
				AuthScope.ANY, // 认证域
				new UsernamePasswordCredentials(InitConfig.CM_USER,
						InitConfig.CM_PSW) // 用户名、密码认证
				);
		httpclient1.executeMethod(putmethod);
	}

	/**
	 * 向cms发送post请求,无请求实体
	 * 
	 * @param url
	 *            请求地址
	 * @throws Exception
	 */
	public void postMethod(String url) throws Exception {
		PostMethod postmethod = new PostMethod(url);
		HttpClient httpclient = new HttpClient();
		httpclient.getState().setCredentials(
				// 服务器认证
				AuthScope.ANY, // 认证域
				new UsernamePasswordCredentials(InitConfig.CM_USER,
						InitConfig.CM_PSW) // 用户名、密码认证
				);
		httpclient.executeMethod(postmethod);
	}

	/**
	 * 向cms发送post请求,有请求实体
	 * 
	 * @param url
	 *            请求地址
	 * @param entity
	 *            字符串类型请求实体
	 * @throws Exception
	 */
	public String postMethod(String url, String entity) throws Exception {
		PostMethod postmethod = new PostMethod(url);
		HttpClient httpclient = new HttpClient();
		httpclient.getState().setCredentials(
				// 服务器认证
				AuthScope.ANY, // 认证域
				new UsernamePasswordCredentials(InitConfig.CM_USER,
						InitConfig.CM_PSW) // 用户名、密码认证
				);
		RequestEntity requestEntity = new ByteArrayRequestEntity(
				entity.getBytes());
		postmethod.setRequestEntity(requestEntity);
		httpclient.executeMethod(postmethod);
		String response = postmethod.getResponseBodyAsString();
		response = new String(response.getBytes("ISO8859-1"), "UTF-8");
		return response;
	}

	/**
	 * 刷新clouderManegerService节点的队列配置，在cm界面修改队列配置保存之后，调用该接口新配置即可生效
	 * 
	 * @return
	 * @throws Exception
	 */
	public String refreshQueue_CMS() throws Exception {
		// String url =
		// "http://192.168.8.206:7180/api/v9/clusters/cluster/services/yarn/roleCommands/refresh"
		String url = InitConfig.CM_ADDR + "/api/v9/clusters/"
				+ getClusterName() + "/services/" + getYarnServiceName()
				+ "/roleCommands/refresh";
		String RMRoleName = getRMRoleName();
		if ("UNKNOWN".equals(RMRoleName)) {
			return RMRoleName;
		}
		String jsonParams = "{'items' : ['" + getRMRoleName() + "']}";
		JSONObject json = new JSONObject(jsonParams);
		String response = postMethod(url, json.toString());
		return response;
	}
}
