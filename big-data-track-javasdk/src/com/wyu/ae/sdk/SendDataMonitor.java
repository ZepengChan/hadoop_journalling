package com.wyu.ae.sdk;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 发送url数据的监控者，用于启动一个单独线程来发送数据
 *
 * @author ken
 * @ClassName: SendDataMonitor
 */
public class SendDataMonitor {

    /**
     * 日志记录对象
     */
    private static final Logger log = Logger.getGlobal();
    /**
     * 单例对象
     */
    private static SendDataMonitor monitor = null;
    /**
     * 阻塞队列，用于存储发送url
     */
    private BlockingQueue<String> queue = new LinkedBlockingQueue<>();

    private SendDataMonitor() {
        // 私有构造方法，单例模式
    }


    /**
     * 获取单例对象
     *
     * @return
     * @Title: getSendDataMonitor
     */
    public static SendDataMonitor getSendDataMonitor() {
        if (monitor == null) {
            synchronized (SendDataMonitor.class) {
                if (monitor == null) {
                    monitor = new SendDataMonitor();
                    Thread thread = new Thread(() -> monitor.run());
                    thread.start();
                }
            }
        }
        return monitor;
    }


    /**
     * 添加一个url到队列中去
     *
     * @param url
     * @throws InterruptedException
     * @Title: addSendUrl
     */
    public static void addSendUrl(String url) throws InterruptedException {
        getSendDataMonitor().queue.put(url);
    }

    /**
     * 具体执行发送url的方法
     *
     * @Title: run
     */
    private void run() {
        while (true) {
            try {
                String url = this.queue.take();
                HttpRequestUtil.sendData(url);
            } catch (Throwable e) {
                log.log(Level.WARNING, "发送url异常", e);
            }
        }
    }

    /**
     * 内部类，用于发送数据的http工具类
     *
     * @author ken
     * @ClassName: HttpRequestUtil
     */
    public static class HttpRequestUtil {

        /**
         * 发送数据到url的具体方法
         *
         * @param url
         * @throws IOException
         * @Title: sendData
         */
        public static void sendData(String url) throws IOException {
            HttpURLConnection con = null;
            BufferedReader in = null;
            try {
                URL obj = new URL(url); // 创建url对象
                con = (HttpURLConnection) obj.openConnection(); // 打开url链接
                // 设置参数
                con.setConnectTimeout(5000); // 连接过期时间
                con.setReadTimeout(5000); // 读取过期时间
                con.setRequestMethod("GET"); // 请求类型为get
                System.out.println("发送url：" + url);
                in = new BufferedReader(new InputStreamReader(con.getInputStream()));

            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                    if (con != null) {
                        con.disconnect();
                    }

                } catch (Throwable e) {
                    // 不做任何处理
                }
            }
        }
    }
}
