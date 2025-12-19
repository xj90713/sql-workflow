import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ShellParser {

    /**
     * 解析 Shell 文本中的 target_tables 列表
     * @param shellContent 完整的 Shell 脚本内容
     * @return 提取到的表名列表
     */
    public static List<String> extractTargetTables(String shellContent) {
        List<String> tables = new ArrayList<>();
        String marker = "##target_tables##";
        int index = shellContent.indexOf(marker);

        if (index == -1) {
            return tables;
        }

        // 2. 截取标识符之后的内容
        String subContent = shellContent.substring(index + marker.length());

        // 3. 使用正则匹配 # 后面紧跟的表名
        // ^#\\s*(\\w+) 匹配行首的 #，忽略可能的空格，捕获单词字符
        Pattern pattern = Pattern.compile("^#\\s*([a-zA-Z0-9_]+)", Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(subContent);

        while (matcher.find()) {
            tables.add(matcher.group(1));
        }
        return tables;
    }

    public static void main(String[] args) {
        String shellScript = """
                #!/bin/bash
                #默认 pt_day=$(date +%F)
                count=1
                NGINX_HOME=/data/apps/openresty/nginx
                DATA_HOME=/formalData/flume/buryPointData
                currentDay=$(date -d ${pt_day}" 0 day" +"%Y%m%d")
                left1Day=$(date -d ${pt_day}" -1 day" +"%y-%m-%d")
                hadoop fs -test -d ${DATA_HOME}/${left1Day}
                if [ $? -eq 0 ]; then
                    hadoop fs -rm -r /formalData/flume/buryPointData/${left1Day}
                    hadoop fs -mkdir /formalData/flume/buryPointData/${left1Day}
                else
                    hadoop fs -mkdir /formalData/flume/buryPointData/${left1Day}
                fi
                local93=$(ssh root@nfbigdata-93 "hadoop fs -put ${NGINX_HOME}/logs/access.log-${currentDay} ${DATA_HOME}/${left1Day}/access.log-${currentDay}-93; ls -l ${NGINX_HOME}/logs/access.log-${currentDay}"|awk '{print $5}')
                local92=$(ssh root@nfbigdata-92 "hadoop fs -put ${NGINX_HOME}/logs/access.log-${currentDay} ${DATA_HOME}/${left1Day}/access.log-${currentDay}-92; ls -l ${NGINX_HOME}/logs/access.log-${currentDay}"|awk '{print $5}')
                local91=$(ssh root@nfbigdata-91 "hadoop fs -put ${NGINX_HOME}/logs/access.log-${currentDay} ${DATA_HOME}/${left1Day}/access.log-${currentDay}-91; ls -l ${NGINX_HOME}/logs/access.log-${currentDay}"|awk '{print $5}')
                hdfs93=0
                hdfs92=0
                hdfs91=0
                until [ $local93 -eq $hdfs93 ] && [ $local92 -eq $hdfs92 ] && [ $local91 -eq $hdfs91 ]
                do
                  hdfs93=$(hadoop fs -ls ${DATA_HOME}/${left1Day}/access.log-${currentDay}-93|awk '{print $5}')
                  hdfs92=$(hadoop fs -ls ${DATA_HOME}/${left1Day}/access.log-${currentDay}-92|awk '{print $5}')
                  hdfs91=$(hadoop fs -ls ${DATA_HOME}/${left1Day}/access.log-${currentDay}-91|awk '{print $5}')
                  sleep 10
                  count=$((count + 1))
                  if [ $count -eq 13 ]; then
                    /data/apps/SparkRuntimeTempDir/WechatSend.sh '执行上传时间超过2分钟'
                    exit 1
                  fi
                done
                spark-submit --class com.spark_scala.app.BuryLogSplitApp \\
                 --master yarn \\
                 --deploy-mode cluster \\
                 --driver-memory 2g \\
                 --executor-memory 4g \\
                 --executor-cores 2 \\
                 --queue default \\
                 hdfs://nameservice1/jars/buryLogProject-1.0-SNAPSHOT.jar ${pt_day}
                
                
                ##target_tables##
                #ods_app_behavior_di_0log
                #ods_app_em_behavior_di_0log 
                #ods_app_heartbeat_di_0log
                #ods_app_user_device_info_di_0log
                #ods_pws_behavior_di_0log
                #ods_mb_behavior_di_0log
                #ods_pc_behavior_di_0log
                #ods_pc_heartbeat_di_0log
                #ods_wpf_error_di_0log
                #ods_wmp_behavior_di_0log
                #ods_wmp_heartbeat_di_0log
                #ods_ios_traffic_reporting_di_0log
                #ods_ios_push_receipt_di_0log
                #ods_tougu_live_video_hb_di_0log
                #ods_financial_sdk_di_0log
                #ods_scrm_behavior_di_2log
                #dim_gzg_bury_data_size_di
                """;

        List<String> tableList = extractTargetTables(shellScript);

        // 打印结果
        tableList.forEach(System.out::println);
    }
}