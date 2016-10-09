package com.test.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

public class JavaUtil {
	/**
	 * 获取时间戳
	 * 
	 * @return 返回时间
	 */
	public static String getTime() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss-SSS");
		String timestamp = sdf.format(new Date());
		return timestamp;
	}

	/**
	 * 创建只有errmsg一个属性的map对象
	 * 
	 * @param msg
	 * @return
	 */
	public static Map<String, String> errMap(String msg) {
		Map<String, String> map = new HashMap<String, String>();
		map.put("errmsg", msg);
		return map;
	}

	/**
	 * 生成根节点下只包含一层节点xml
	 * 
	 * @param rootElement
	 *            根节点
	 * @param map
	 *            存储节点信息
	 * @return
	 */
	public static String createXml1(String rootElementName,
			Map<String, String> map) throws Exception {
		Document document = DocumentHelper.createDocument();
		Element rootElement = document.addElement(rootElementName);

		Set<String> keys = map.keySet();
		Iterator<String> it = keys.iterator();
		while (it.hasNext()) {
			String key = it.next();
			String value = map.get(key);
			Element element = rootElement.addElement(key);
			element.setText(value);
		}
		return formatXML(document);
	}

	/**
	 * 生成根节点下只包含两层节点的xml
	 * 
	 * @param rootElement
	 *            根节点
	 * @param map
	 *            存储节点信息
	 * @return
	 */
	public static String createXml2(String rootElement,
			Map<String, Map<String, String>> map) throws Exception {

		Document document = DocumentHelper.createDocument();
		Element rootelement = document.addElement(rootElement);

		Set<String> keys = map.keySet();
		Iterator<String> it = keys.iterator();
		while (it.hasNext()) {
			String key = it.next();
			Map<String, String> values = map.get(key);
			Element elements = rootelement.addElement(key);
			Set<String> keys1 = values.keySet();
			Iterator<String> it1 = keys1.iterator();
			while (it1.hasNext()) {
				String key1 = it1.next();
				String value1 = values.get(key1);
				if (values.size() == 1) {
					elements.setText(value1);
				}
				if (values.size() > 1) {
					Element element1 = elements.addElement(key1);
					element1.setText(value1);
				}
			}
		}
		return formatXML(document);
	}

	/**
	 * 读文件内容
	 * 
	 * @param filePath
	 *            文件路径
	 * @return String类型的文件内容
	 * @throws Exception
	 */
	public static String convertFileToString(String filePath) throws Exception {
		File file = new File(filePath);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String data = "";
		String dataTemp = null;
		// 一次读入一行，直到读入null为文件结束
		while ((dataTemp = reader.readLine()) != null) {
			data += "\n" + dataTemp;
		}
		reader.close();
		data = data.substring(1);
		return data;
	}

	/**
	 * 将输入量转换为String
	 * 
	 * @param is
	 *            输入流
	 * @return
	 */
	public static String convertStreamToString(InputStream is) {
		/*
		 * To convert the InputStream to String we use the
		 * BufferedReader.readLine() method. We iterate until the BufferedReader
		 * return null which means there's no more data to read. Each line will
		 * appended to a StringBuilder and returned as String.
		 */
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		StringBuilder sb = new StringBuilder();

		String line = null;
		try {
			while ((line = reader.readLine()) != null) {
				sb.append(line + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return sb.toString();
	}

	/**
	 * 递归删除目录下的所有文件及子目录下所有文件
	 * 
	 * @param dir
	 *            将要删除的文件目录.
	 */
	public static boolean deleteDir(String dirPath) {
		File dir = new File(dirPath);
		if (dir.isDirectory()) {
			String[] children = dir.list();
			// 递归删除目录中的子目录下
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(dirPath + "/" + children[i]);
				if (!success) {
					return false;
				}
			}
		}
		return dir.delete(); // 目录此时为空，可以删除
	}

	/**
	 * 检验是否存在本地文件
	 * 
	 * @param localFilePath
	 *            本地文件路径
	 * @return
	 */
	public static boolean existLocalFile(String localFilePath) {
		File checksumLocal = new File(localFilePath);
		if (checksumLocal.exists()) {
			return true;
		}
		return false;
	}

	/**
	 * 格式化xml
	 * 
	 * @param document
	 *            需要格式化的xml document
	 * @return 返回格式化后的xml字符串
	 * @throws Exception
	 */
	public static String formatXML(Document document) throws Exception {
		StringWriter strWtr = new StringWriter();
		OutputFormat format = OutputFormat.createPrettyPrint();
		format.setEncoding("UTF-8");
		XMLWriter xmlWriter = new XMLWriter(strWtr, format);
		xmlWriter.write(document);
		String result = strWtr.toString();
		return result;
	}

	/**
	 * 发送get请求,不需要服务器认证
	 * 
	 * @param url
	 *            请求地址
	 * @return cms返回信息
	 * @throws Exception
	 */
	public static String getMethod(String url) throws Exception {
		GetMethod getmethod = new GetMethod(url);
		HttpClient httpclient = new HttpClient();
		httpclient.executeMethod(getmethod);
		String s = getmethod.getResponseBodyAsString();
		s = new String(s.getBytes("ISO8859-1"), "UTF-8");
		return s;
	}

	/**
	 * 将异常写入文件
	 * 
	 * @param e
	 *            异常
	 * @param filePath
	 *            文件路径
	 * @throws Exception
	 */
	public static void writeExceptionToFile(Exception e, String filePath)
			throws Exception {
		FileWriter fw = new FileWriter(filePath, true);
		PrintWriter pw = new PrintWriter(fw, true);
		e.printStackTrace(pw);
		pw.flush();
		fw.flush();
		pw.close();
		fw.close();
	}

	/**
	 * 将输入流写入文件
	 * 
	 * @param in
	 *            输入流
	 * @param filePath
	 *            文件路径
	 * @throws Exception
	 *             抛出异常
	 */
	public static void writeInputStreamToFile(InputStream in, String filePath)
			throws Exception {
		File file = new File(filePath);
		File parentFile = file.getParentFile();
		if (!parentFile.exists()) {
			parentFile.mkdirs();
		}
		OutputStream ous = new FileOutputStream(filePath, true);
		byte[] buffer = new byte[1024]; // 定义缓冲区
		int byteread = 0;
		while ((byteread = in.read(buffer)) != -1) {
			ous.write(buffer, 0, byteread);
		}
		ous.close();
		in.close();
	}

	/**
	 * 将文本写入文件
	 * 
	 * @param context
	 *            要写入的文本
	 * @param filePath
	 *            文件路径
	 * @throws Exception
	 */
	public static void log(String context, String filePath) throws Exception {
		context = getTime() + "	" + context;
		File file = new File(filePath);
		File parentErr = file.getParentFile();
		if (!parentErr.exists()) {
			parentErr.mkdirs();
		}
		FileWriter fw = new FileWriter(file, true);
		fw.write(context + "\n");
		fw.flush();
		fw.close();
	}

	/**
	 * 取最小值
	 * 
	 * @param param1
	 *            参数1
	 * @param param2
	 *            参数2
	 * @return 返回最小值
	 */
	public static int min(int param1, int param2) {
		int min;
		min = param1 < param2 ? param1 : param2;
		return min;
	}

	/**
	 * 取最大值
	 * 
	 * @param param1
	 *            参数1
	 * @param param2
	 *            参数2
	 * @return 返回最大值
	 */
	public static int max(int param1, int param2) {
		int max;
		max = param1 > param2 ? param1 : param2;
		return max;
	}

	/**
	 * 解压zip文件
	 * 
	 * @param srcZipFile
	 *            需要解压的Zip包
	 * @param unzipPath
	 *            解压输出路径
	 * @param stdoutPath
	 *            日志信息输出路径
	 */
	public static void unzip(File srcZipFile, String unzipPath,
			String stdoutPath) {
		try {
			ZipInputStream Zin = new ZipInputStream(new FileInputStream(
					srcZipFile), Charset.forName("GBK"));// 输入源zip路径
			BufferedInputStream bis = new BufferedInputStream(Zin);
			String Parent = unzipPath; // 输出路径（文件夹目录）
			File fout = null;
			ZipEntry entry;
			while ((entry = Zin.getNextEntry()) != null) {
				if (entry.isDirectory()) {
					continue;
				}
				fout = new File(Parent, entry.getName());
				if (!fout.exists()) {
					(new File(fout.getParent())).mkdirs();
				}
				FileOutputStream fos = new FileOutputStream(fout);
				BufferedOutputStream bos = new BufferedOutputStream(fos);
				int length;
				byte[] buffer = new byte[1024];
				while ((length = bis.read(buffer)) != -1) {
					bos.write(buffer, 0, length);
				}
				bos.close();
				fos.close();
				System.out.println(fout + "解压成功");
			}
			bis.close();
			Zin.close();
		} catch (Exception e) {
			try {
				writeExceptionToFile(e, stdoutPath);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
	}

}
