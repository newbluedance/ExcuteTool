# -*- coding: utf-8 -*-

from PyQt5 import QtCore
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QWidget, QComboBox, QLabel, QPushButton, QTextBrowser, QCalendarWidget, QStatusBar, \
    QApplication, QMainWindow
import os
import yaml
import paramiko
import sys
import time
import pymysql
import datetime
import re

with open('config.yml', encoding='UTF-8') as f:
    info = yaml.load(f)

dic_job_type = info['jobType']
list_job_type = list(dic_job_type.keys())
dic_env = info['env']
list_env = list(dic_env.keys())
port = info['port']

#判断是否仍在生成数据
def lastCreatTime(table):
    db = pymysql.connect('cs-expert.yazuoyw.com','cs','Password123',table)
    cursor = db.cursor()
    sql = 'SELECT create_time from cpt_abnormal_store_detail ORDER BY create_time DESC Limit 1'
    cursor.execute(sql)
    results = cursor.fetchall()
    last_create_time = results[0][0]
    return last_create_time

class ui_excute(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(511, 475)
        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.envComboBox = QComboBox(self.centralwidget)
        self.envComboBox.setGeometry(QtCore.QRect(160, 30, 111, 21))
        self.envComboBox.setObjectName("envComboBox")
        self.typeComboBox = QComboBox(self.centralwidget)
        self.typeComboBox.setGeometry(QtCore.QRect(160, 80, 111, 21))
        self.typeComboBox.setObjectName("typeComboBox")
        self.label = QLabel(self.centralwidget)
        self.label.setGeometry(QtCore.QRect(40, 30, 91, 16))
        self.label.setObjectName("label")
        self.label_2 = QLabel(self.centralwidget)
        self.label_2.setGeometry(QtCore.QRect(40, 80, 91, 16))
        self.label_2.setObjectName("label_2")
        self.label_3 = QLabel(self.centralwidget)
        self.label_3.setGeometry(QtCore.QRect(40, 130, 91, 16))
        self.label_3.setObjectName("label_3")
        self.pushButton = QPushButton(self.centralwidget)
        self.pushButton.setGeometry(QtCore.QRect(410, 365, 81, 41))
        self.pushButton.setObjectName("pushButton")
        self.pushButton_1 = QPushButton(self.centralwidget)
        self.pushButton_1.setGeometry(QtCore.QRect(30, 365, 81, 41))
        self.pushButton_1.setObjectName("checkPushButton")
        self.textBrowser = QTextBrowser(self.centralwidget)
        self.textBrowser.setGeometry(QtCore.QRect(160, 350, 240, 90))
        self.textBrowser.setObjectName("textBrowser")
        self.calendarWidget = QCalendarWidget(self.centralwidget)
        self.calendarWidget.setGeometry(QtCore.QRect(160, 130, 271, 200))
        self.calendarWidget.setGridVisible(True)
        self.calendarWidget.setHorizontalHeaderFormat(QCalendarWidget.ShortDayNames)
        self.calendarWidget.setVerticalHeaderFormat(QCalendarWidget.NoVerticalHeader)
        self.calendarWidget.setNavigationBarVisible(True)
        self.calendarWidget.setDateEditEnabled(True)
        self.calendarWidget.setObjectName("calendarWidget")
        MainWindow.setCentralWidget(self.centralwidget)
        self.statusbar = QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)

        self.retranslateUi(MainWindow)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

        for i in list_env:
            self.envComboBox.insertItem(list_env.index(i), i)
        for i in list_job_type:
            self.typeComboBox.insertItem(list_job_type.index(i), i)

        self.pushButton.clicked.connect(self.excutejob)
        self.pushButton_1.clicked.connect(self.checkScanJob)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "定时任务执行工具"))
        self.label.setText(_translate("MainWindow", "定时任务环境："))
        self.label_2.setText(_translate("MainWindow", "定时任务类型："))
        self.label_3.setText(_translate("MainWindow", "定时任务日期："))
        self.pushButton.setText(_translate("MainWindow", "执行"))
        self.pushButton_1.setText(_translate("MainWindow", "服务扫描检查"))

    def checkScanJob(self):
        table = dic_env[self.envComboBox.currentText()]['tablename']
        last_create_time = lastCreatTime(table)
        if ('beginTime' in globals()):
            if last_create_time > beginTime:
                QTextBrowser.append(self.textBrowser, "服务扫描任务已经执行完毕!")
            else:
                QTextBrowser.append(self.textBrowser, "服务扫描任务仍在执行，请稍后再检查!")
        else:
            QTextBrowser.append(self.textBrowser, "请先执行服务扫描任务")

    def excutejob(self):
        #获取参数
        job_env = self.envComboBox.currentText()
        job_type = dic_job_type[self.typeComboBox.currentText()]
        job_name = self.typeComboBox.currentText()
        date = self.calendarWidget.selectedDate()
        job_date = date.toString(Qt.ISODate)
        ip = dic_env[job_env]['ip']
        username = dic_env[job_env]['username']
        password = dic_env[job_env]['password']

        # 连接SSH
        try:
            ssh = paramiko.SSHClient()
            key = paramiko.AutoAddPolicy()
            ssh.set_missing_host_key_policy(key)
            ssh.connect(ip, 22, username, password, timeout=5)

            # 执行Job命令
            job_command = 'export app_name="cs-expertsystem-job";app_job="cs-expertsystem-job-crontab";port=' + str(
                port)+ ';parameter='+job_type+';parameter2='+job_date+';PATH=/usr/java/jdk1.8.0_73/bin:$PATH;JAVA_HOME=/usr/java/jdk1.8.0_73;mkdir -p /yazuo_apps/logs/$app_name/;chmod a+x /yazuo_apps/$app_name/current/$app_name.jar;java -server -Xms512m -Xmx512m -XX:MaxPermSize=128m -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintClassHistogram -XX:+PrintTenuringDistribution -XX:+PrintGCApplicationStoppedTime  -Xloggc:/yazuo_apps/logs/$app_name/gc_log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=100 -XX:GCLogFileSize=10240K -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/yazuo_apps/logs/$app_name/heap_dump.hprof -Dfile.encoding=UTF-8  -Dapp_home=/yazuo_apps/$app_name/current -jar /yazuo_apps/$app_name/current/$app_name.jar $parameter $parameter2 --server.port=$port  --spring.application.name=$app_name --logging.path=/yazuo_apps/logs/$app_name/ --logging.file=/yazuo_apps/logs/$app_name/info.log --task.name=$app_job --server.tomcat.accesslog.suffix=.log >/yazuo_apps/logs/$app_name/console.log 2>&1'
            stdin, stdout, stderr = ssh.exec_command(job_command)
            ssh.close()
            msg = job_name + '(' + job_date + ") 定时任务发送完毕！"
            QTextBrowser.setText(self.textBrowser, msg)
        except:
            QTextBrowser.setText(self.textBrowser, "执行失败！")

        # 获取日志文件
        t = paramiko.Transport((ip, 22))
        t.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(t)
        beginString = job_type + '任务开始时间'
        cmpleteString = job_type + '执行完毕时间'
        time.sleep(1)
        try:
            state = 0
            time.sleep(5)
            for i in range(1, 120):
                time.sleep(5)
                sftp.get('/yazuo_apps/logs/cs-expertsystem-job/console.log', os.getcwd() + '/\\console.log')
                log_content = open('console.log', 'r', encoding='UTF-8')
                log_string = log_content.read()
                log_content.close()
                if state == 0:
                    if beginString in log_string:
                        state = 1
                        if job_type == 'ScanBizOpportunityJob':
                            break
                if state == 1:
                    if cmpleteString in log_string:
                        state = 2
                        break
            if state == 0:
                QTextBrowser.append(self.textBrowser, "未检测到任务开始执行")
            if state == 1 and job_type == 'ScanBizOpportunityJob':
                mat = re.search(beginString + r'.{21}', log_string)
                global  beginTime
                beginTime = datetime.datetime.strptime(mat.group(0)[-19:],'%Y-%m-%d %H:%M:%S')
                print(beginTime)
                QTextBrowser.append(self.textBrowser, "请点击 服务扫描查询 按钮来检查是否执行完毕")
            if state == 1 and job_type != 'ScanBizOpportunityJob':
                mat = re.search(beginString + r'.{21}', log_string)
                QTextBrowser.append(self.textBrowser, job_type + mat.group(0)[-27:] + " 但未检测到执行结束")
            if state == 2:
                mat = re.search(beginString + r'.{21}', log_string)
                mat1 = re.search(cmpleteString + r'.{21}', log_string)
                QTextBrowser.append(self.textBrowser, job_name + mat.group(0)[-27:])
                QTextBrowser.append(self.textBrowser, job_name + mat1.group(0)[-27:])
        except:
            QTextBrowser.append(self.textBrowser, "获取日志失败，可能未执行成功")

if __name__ == '__main__':
    app = QApplication(sys.argv)
    MainWindow = QMainWindow()
    ui = ui_excute()
    ui.setupUi(MainWindow)
    MainWindow.show()
    sys.exit(app.exec_())