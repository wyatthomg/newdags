# -*- coding: utf-8 -*-
"""
Created on Mon Nov  1 17:18:38 2021

@author: Admin
"""
import pymysql
from sqlalchemy import create_engine
import pendulum
from datetime import timedelta
import requests
import pandas as pd
import json
import warnings
import datetime
import os
import re
from selenium import webdriver
import hashlib
import time
from selenium.webdriver.chrome.service import Service
from selenium.webdriver import Remote
from selenium.webdriver.chrome import options
from selenium.common.exceptions import InvalidArgumentException
import redis 
import pytesseract
from PIL import Image
from tempfile import TemporaryFile,NamedTemporaryFile
import paramiko
import logging

logging.getLogger("paramiko").setLevel(logging.WARNING)
pool = redis.ConnectionPool(host='192.168.100.44', port=7379, decode_responses=True,db=10,password=123456)   # host是redis主机，需要redis服务端和客户端都起着 redis默认端口是6379
r = redis.Redis(connection_pool=pool)
class MySQLconnect():
    def __init__(self , dbname:str,user='zhenggantian'):
        if isinstance(dbname,str):
            self.dbname=dbname
            self.host='192.168.100.33'
            self.user=user
        else :
            raise TypeError ("dbname must be a string")
            
    def __enter__(self,):
        self.eng=self.engine()#sqlal
        # self.engcon=self.engine().connect()#sqlal
        self.con=self.connect()#py
        self.cur=self.con.cursor()#全量插入
        return self
        
    def __exit__(self,exc_type,exc_val,exc_tb):
        # self.engcon.close()
        self.eng.dispose()
        self.cur.close()
        self.con.close()
        if exc_val:
            raise
    
    def engine(self):
         return create_engine(f"mysql+pymysql://{self.user}:123456@{self.host}:3306/{self.dbname}", encoding='utf8')#
    
    def connect(self):
        return pymysql.connect(host=self.host, port=3306, database=self.dbname, user="zhenggantian", password="123456", charset="utf8")

class BiphpFactory():
    """
    all_url()类方法:返回biphp接口列表 \r
    实例:判断act是否存在biphp接口列表，请传递参数名称选填判断 \r
    catch()实例方法:输入参数，参数名(非必须)通过Json解析返回df \r
    refactory()实例方法:输入参数，参数名(非必须)通过正则表达式获取返回df \r
    md5_encrypt()实例方法:加密 \r
    check()实例方法:检查md5_encrypt字段与数据库md_5表对应字段是否相等,相等返1，不等返回2  关键参数 表名 ,中文表名\r
    md5_insert()实例方法:插入数据进md_5表，返回影响行数 关键参数 时间，表名 \r
    md5_updata()实例方法:md_5表数据更新，返回影响行数 关键参数 时间，表名 \r
    format_num()实例方法:返回dataframe列的数量的 %s \r
    add_renew()实例方法:针对参数为时间且表内有时间列的情况下增量更新 \r
    enforce_renew()实例方法:针对参数为时间且表内有时间列的情况下强制更新
    """
    @property
    def all_url():
        with MySQLconnect('mission') as mis:
            sql="""SELECT * FROM `biphp_port_list`"""
            parm_list=pd.read_sql(sql,mis.con)
            return parm_list
    
    
    def __init__(self,act):
        with MySQLconnect('ods') as ods:
            if not isinstance(act,str):
                raise TypeError ('act must be strting ')

            self.act=act
            self.baseurl='https://cp.maso.hk:4433/index.php?main=biphp&key=JJ57S744ZJR26ORGRMMSJ8V4D4UVF5AU&act='
            self.usere=0
            sql=f"""SELECT * FROM `mission`.`biphp_port_list` WHERE act='{self.act}'"""
            self.parm_list=pd.read_sql(sql,ods.con)
            if self.parm_list.empty == True:
                raise ValueError ('act不存在')
            if len(eval(self.parm_list['param'][0]))== 1:
                self.paramname=sorted(eval(self.parm_list['param'][0]).keys())[0]
                self.needparam=0
            else:
                self.needparam=1


        
        
    def catch(self,param,paramname=None):
        if self.needparam == 0:
            pass
        if self.needparam == 1 and paramname in sorted(eval(self.parm_list['param'][0]).keys()):
            self.paramname=paramname
            
            
        self.param=str(param)
        self.url=self.baseurl+self.act+'&'+self.paramname+'='+str(self.param)
        if self.usere==0:
            try:
                self.response = requests.get(url=self.url).text
                self.md5_encrypt()
                response_dict=json.loads(self.response)
                if response_dict['code']=='1':
                    e = response_dict['msg']
                    print(e)
                    data=response_dict['data']
                    self.df=pd.DataFrame(data)
                    return self.df
                else:
                    data=response_dict['data']
                    self.df=pd.DataFrame(data)
                    return self.df
            except:
                self.usere=1
                return self.refactory(self.param)
        elif self.usere==1:
            return self.refactory(self,param,paramname=None)
        
        


    def refactory(self,param,paramname=None):
        if self.needparam == 0:
            pass
        if self.needparam == 1 and paramname in sorted(eval(self.parm_list['param'][0]).keys()):
            self.paramname=paramname
            
        if not isinstance(param,str):
            raise TypeError ('param must be strting ')
            
        self.param=param
        self.url=self.baseurl+self.act+'&'+self.paramname+'='+str(self.param)
        response = requests.get(url=self.url).text
        if self.act=='store_out':
            title_list=['id','store_in_id','out_id','store_name','products_id','products_name','standard_name','standard_id','out_detials_qty','out_verification','out_maker','out_time','out_detials_outlink_id','maker_name','site_name']
            re_link= '"id":"(.*?)","store_in_id":"(.*?)","out_id":"(.*?)","store_name":"(.*?)","products_id":"(.*?)","products_name":"(.*?)","standard_name":"(.*?)","standard_id":"(.*?)","out_detials_qty":"(.*?)","out_verification":"(.*?)","out_maker":"(.*?)","out_time":"(.*?)","out_detials_outlink_id":"(.*?)","maker_name":"(.*?)","site_name":"(.*?)"'
        elif self.act=='stock_in':
            title_list=['id','stock_in_id','store_name','product_id','products_name','standard_name','standard_id','instock_num','in_details_surplus_qty','price','in_verification','maker_name','in_store_time']
            re_link='"id":"(.*?)","stock_in_id":"(.*?)","store_name":"(.*?)","product_id":"(.*?)","products_name":"(.*?)","standard_name":"(.*?)","standard_id":"(.*?)","instock_num":"(.*?)","in_details_surplus_qty":"(.*?)","price":"(.*?)","in_verification":"(.*?)","maker_name":"(.*?)","in_store_time":"(.*?)"'
        elif self.act=='order_products':
            title_list=['order_id','original_order_id','order_time','site_id','site_name','cate_id','cate_name','suborder_id','product_id','standard_name_1','standard_id','final_price_dollar','final_price','currency','product_quantity','delivery_country','standard_name_2']
            re_link='"order_id":"(.*?)","original_order_id":"(.*?)","order_time":"(.*?)","site_id":"(.*?)","site_name":"(.*?)","cate_id":"(.*?)","cate_name":"(.*?)","suborder_id":"(.*?)","product_id":"(.*?)","standard_name_1":"(.*?)","standard_id":"(.*?)","final_price_dollar":"(.*?)","final_price":"(.*?)","currency":"(.*?)","product_quantity":"(.*?)","delivery_country":"(.*?)","standard_name_2":"(.*?)"'
        elif self.act=='warehouse_purchasing':
            title_list=['order_product_id','trans_id','site_name','type_1','product_id','product_sp','product_num','fun_audit','product_audit','order_audit','product_log_audit','order_log_audit','client_pay_date','checkout_date','check_operator','frist_bill_date','last_bill_date','bill_operator','outstock_date','outstock_operator','packing_date','packer','buy_id','buy_audit','buy_num','system_price','actual_price','trans_operator','trans_add_date','trans_pay_date','trans_log_add_date','log_sign_date','log_signner','log_receiving_date','wait_handsout','log_receiver','log_handout','log_handout_packed','handout_packer','outpacking_date','outpacker','delivery_date','delivery_operator','package','get_package','cooperation','buy_id_add_date','confirm_amount_operator','confirm_date','waybill_sign_date','waybill_log_date','product_sp_id','order_time']
            re_link='"order_product_id":"(.*?)","trans_id":"(.*?)","site_name":"(.*?)","cate_3":"(.*?)","product_id":"(.*?)","product_sp":"(.*?)","product_num":"(.*?)","fun_audit":"(.*?)","product_audit":"(.*?)","order_audit":"(.*?)","product_log_audit":"(.*?)","order_log_audit":"(.*?)","client_pay_date":"(.*?)","checkout_date":"(.*?)","check_operator":"(.*?)","frist_bill_date":"(.*?)","last_bill_date":"(.*?)","bill_operator":"(.*?)","outstock_date":"(.*?)","outstock_operator":"(.*?)","packing_date":"(.*?)","packer":"(.*?)","buy_id":"(.*?)","buy_audit":"(.*?)","buy_num":"(.*?)","system_price":"(.*?)","actual_price":"(.*?)","trans_operator":"(.*?)","trans_add_date":"(.*?)","trans_pay_date":"(.*?)","trans_log_add_date":"(.*?)","log_sign_date":"(.*?)","log_signner":"(.*?)","log_receiving_date":"(.*?)","wait_handsout":"(.*?)","log_receiver":"(.*?)","log_handout":"(.*?)","log_handout_packed":"(.*?)","handout_packer":"(.*?)","outpacking_date":"(.*?)","outpacker":"(.*?)","delivery_date":"(.*?)","delivery_operator":"(.*?)","package":"(.*?)","get_package":"(.*?)","cooperation":"(.*?)","buy_id_add_date":"(.*?)","confirm_amount_operator":"(.*?)","confirm_date":"(.*?)","waybill_sign_date":"(.*?)","waybill_log_date":"(.*?)","standard_id":"(.*?)","order_time":"(.*?)"'
        elif self.act=='ware_trans_list':
            title_list=['id','batch_id','applicant','batch_add_date','pre_purchase_date','act_purchase_date','pre_logpickup_date','act_logpickup_date','pre_intostock_date','act_intostock_date','limitation','status','note','outstock_num','batch_audit_date','buy_id','buy_audit','cate_3','product_id','product_sp','product_sp_id','warehouse_name','buy_num','system_price','actual_price','operator','trans_add_date','trans_pay_date','trans_log_add_date','log_sign_date','log_signner','log_receiving_date','purchase_completed_date','log_receiver','intostock_date','intostock_operator','handout_date','handout_packer','outpacking_date','outpacker','delivery_date','delivery_operator','sku_price','standard_id']
            re_link='"id":"(.*?)","batch_id":"(.*?)","applicant":"(.*?)","batch_add_date":"(.*?)","pre_purchase_date":"(.*?)","act_purchase_date":"(.*?)","pre_logpickup_date":"(.*?)","act_logpickup_date":"(.*?)","pre_intostock_date":"(.*?)","act_intostock_date":"(.*?)","limitation":"(.*?)","status":"(.*?)","note":"(.*?)","outstock_num":"(.*?)","batch_audit_date":"(.*?)","buy_id":"(.*?)","buy_audit":"(.*?)","cate_3":"(.*?)","product_id":"(.*?)","product_sp":"(.*?)","product_sp_id":"(.*?)","warehouse_name":"(.*?)","buy_num":"(.*?)","system_price":"(.*?)","actual_price":"(.*?)","operator":"(.*?)","trans_add_date":"(.*?)","trans_pay_date":"(.*?)","trans_log_add_date":"(.*?)","log_sign_date":"(.*?)","log_signner":"(.*?)","log_receiving_date":"(.*?)","purchase_completed_date":"(.*?)","log_receiver":"(.*?)","intostock_date":"(.*?)","intostock_operator":"(.*?)","handout_date":"(.*?)","handout_packer":"(.*?)","outpacking_date":"(.*?)","outpacker":"(.*?)","delivery_date":"(.*?)","delivery_operator":"(.*?)","sku_price":"(.*?)","standard_id":"(.*?)"'
        elif self.act=='start_order':
            title_list=["id","order_id","site_id","convey","order_time","ordercate","currency","currency_rate","order_freight_price_dollar","order_price_dollar","postcode","delivery_country"]
            re_link='{"id":"(.*?)","order_id":"(.*?)","site_id":"(.*?)","convey":"(.*?)","order_time":"(.*?)","ordercate":"(.*?)","currency":"(.*?)","currency_rate":"(.*?)","order_freight_price_dollar":"(.*?)","order_price_dollar":"(.*?)","postcode":"(.*?)","delivery_country":"(.*?)"}'
        pattern=re.compile(re_link)
        result=pattern.findall(response)
        self.df=pd.DataFrame(result,columns=title_list)
        return self.df

        
    def md5_encrypt(self):#文本加密
        self.md5_encrypt=hashlib.md5(str(self.response).encode()).hexdigest()
            

    def check(self,date,tablename,table_chinese):
        self.date=date
        self.tablename=tablename
        self.table_chinese=table_chinese
        with MySQLconnect('ods') as ods: 
            sql=f"""SELECT DISTINCT date FROM `ods`.`md_5` WHERE  `table`= '{self.tablename}'  and date='{self.date}'"""
            now_date=pd.read_sql(sql,ods.con)
            if now_date.empty == True:
                return self.md5_insert()
            else:
                return self.md5_updata()

    def md5_insert(self):
        with MySQLconnect('ods') as ods: 
            sql = 'INSERT IGNORE INTO md_5 (`date`,`md5`,`table`,`table_ch`,`times`) VALUES (%s,%s,%s,%s,%s)'
            ods.cur.execute(sql, (self.date, self.md5_encrypt, self.tablename, self.table_chinese, None))
            ods.con.commit()
            affected_rows = ods.cur.rowcount
            print('\n状态:正常更新\n类型: 插入\nmd_5影响行数:',affected_rows,'\n时间:',self.date)
            return affected_rows
        
    def md5_updata(self):
        with MySQLconnect('ods') as ods: 
            sql = "UPDATE md_5 SET md5 = CASE WHEN `table` = '%s' and `date` ='%s'and `md5`!='%s' THEN '%s' else `md5` END"%(self.tablename,self.date,self.md5_encrypt,self.md5_encrypt)
            ods.cur.execute(sql)
            ods.con.commit()
            affected_rows = ods.cur.rowcount
            print('\n状态:正常更新\n类型: 更新\nmd_5影响行数:',affected_rows,'\n时间:',self.date)
            return affected_rows
    
    def format_num(self):
            df_columns=self.df.shape[1]
            self.format_num = "(%s"+",%s"*(df_columns-1)+")"
            return self.format_num

    def column_name(self):

        column_names=[column for column in self.df]
        self.column_name=str(column_names).replace('[','(').replace(']',')').replace('\'','`')
        return self.column_name

    def add_renew(self,table_time,dbname:str):
        self.column_name()
        self.format_num()
        self.table_time=table_time
        self.dbname=dbname
        with MySQLconnect(self.dbname) as db: 
            sql_delete=f"DELETE FROM {self.tablename} WHERE date(`{self.table_time}`) = '{self.param}'"
            sql_insert="INSERT IGNORE INTO %s %s VALUES %s"%(self.tablename,self.column_name,self.format_num)
            tuple_data= [tuple(row) for row in self.df.values]#取值转为sql所需元组
            try:
                db.cur.execute(sql_delete)
                db.cur.executemany(sql_insert,tuple_data)
                db.con.commit()
                affected_rows = db.cur.rowcount
                print(f'{self.tablename}影响行数:{affected_rows}')
            except Exception as e:
                print(e)
                db.con.rollback
    def enforce_renew(self,tablename,table_chinese,table_time,dbname:str):
        self.column_name()
        self.format_num()
        self.dbname=dbname
        with MySQLconnect(self.dbname) as db: 
            sql_delete=f"DELETE FROM {tablename} WHERE date(`{table_time}`) = '{self.param}'"
            sql_insert="INSERT IGNORE INTO %s  %s VALUES %s"%(tablename,self.column_name,self.format_num)
            md5_delete=f"DELETE FROM `md_5` WHERE`table`='{tablename}'AND date = '{self.param}'"
            md5_insert=f"INSERT  INTO `md_5` (date,md5,`table`,table_ch) VALUES ('{self.param}','{self.md5_encrypt}','{tablename}','{table_chinese}')"
            tuple_data= [tuple(row) for row in self.df.values]
            try:
                db.cur.execute(md5_delete)
                db.cur.execute(md5_insert)
                db.cur.execute(sql_delete)
                db.cur.executemany(sql_insert,tuple_data)
                db.con.commit()
                affected_rows = db.cur.rowcount
                print(f'\n状态:强制更新\n时间:{self.param} \nmd_5:强制更新 \n{tablename}:强制更新 \n{tablename}影响行数:{affected_rows}')
            except Exception as e:
                print(e)
                db.con.rollback
    
local_tz = pendulum.timezone("Asia/Shanghai")
default_args = {
    'owner': 'hadoop03',
    'start_date': pendulum.datetime(year=2021,month=1,day=1,tz=local_tz),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}   





def web_browser(run=0,port=9222):
    if run==1:
        service=Service('/usr/bin/chromedriver')#谷歌驱动路劲
        service.command_line_args()
        service.start()
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('ignore-certificate-errors')
        options.add_argument('--start-maximized')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-gpu')
        options.add_argument(' -port=9222')
        driver=webdriver.Remote(service.service_url,options=options)
        return driver
    else :
        pass
    

    
class ReuseChrome(Remote):

    def __init__(self, command_executor, session_id):
        self.r_session_id = session_id
        Remote.__init__(self, command_executor=command_executor, desired_capabilities={})

    def restart_session(self, capabilities, browser_profile=None):
        """
        重写start_session方法
        """
        if not isinstance(capabilities, dict):
            raise InvalidArgumentException("capabilities must be a dictionary")
        if browser_profile:
            if "moz:firefoxOptions" in capabilities:
                capabilities["moz:firefoxOptions"]["profile"] = browser_profile.encoded
            else:
                capabilities.update({'firefox_profile': browser_profile.encoded})

        self.capabilities = options.Options().to_capabilities()
        self.session_id = self.r_session_id
        self.w3c = False


def Vc():
    while 1:
        urls = 'https://cp.maso.hk:4433/index.php?main=login&act=vercode'#图片链接（每秒更新，距离当前时间最近的时间戳最近为可使用图片）
        res = requests.post(url=urls)
        ress = re.findall('Cookie (.*?) for',str(res.cookies))#获取图片链接Cooike，图片Cooike必须和登录请求的Cooike保持一致
        
        f1 = NamedTemporaryFile(mode='wb+',suffix='.png')
        f1.write(res.content)#导入文件路径（待处理图片）
        f1.seek(0)
        img = Image.open(f1)#未处理图片
        # plt.imshow(img)
        # plt.show
        lim = img.convert('L')
        threshold = 165 # 灰度阈值设为165(可调整)，低于这个值的点全部填白色
        table = []
        for j in range(256):
            if j < threshold:
                table.append(1)
            else:
                table.append(0)
        bim = lim.point(table, '1')#连接各个且分片儿
        f2 = NamedTemporaryFile(mode='wb+',suffix='.png')
        bim.save(f2)#处理完成导入
        f2.seek(0)
        text = pytesseract.image_to_string(Image.open(f2))#开始识别
        url = 'https://cp.maso.hk:4433/index.php?main=login&act=check'
        haders = {
            'Cookie': ress[0]   #使用之前的图片链接Cooike
        }
        data = {
            'user': 'honghuayuan',
            'pswd': 'a12345',
            'vercode': text.replace('\n',''), #识别后的验证码结果
            'issub': '0',
            'subuser': ''
        }
        r = requests.post(url=url,headers=haders,data=data).text# 尝试使用验证码登录
        if r != 'success':
            print('验证码错误正在尝试重新识别验证码')
            # time.sleep(1)
            continue
        else:
            return ress[0]
            break



def errorpush(context,uid=10818):
    ti = context['task_instance']
    text =f"{context['ds']} : task <{ti.task_id }> fail in dag <{ ti.dag_id }> "
    import requests
    url1 ='https://api.baycheer.com/Notice/message'
    msg={'app_id':'392010',
        'app_token':'RK1XW5WW9YYLGKH68N7T1NAGD206TVZ8',
        'toWxWork':'true',
        'uid':uid,
        'message':text
        }
    requests.post(url=url1,data=msg)


def transportfile(file,path):
    transport = paramiko.Transport('hadp04', 22)
    transport.connect(username='root', password='123456')
    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.put(file,path)
    transport.close()



class Linux():
    # 通过IP, 用户名，密码，超时时间初始化一个远程Linux主机
    def __init__(self, ip='192.168.100.44', username='root', password='123456', timeout=30):
        self.ip = ip
        self.username = username
        self.password = password
        self.timeout = 30
        # transport和chanel
        self.t = ''
        self.chan = ''
        # 链接失败的重试次数
        self.try_times = 3


    # 调用该方法连接远程主机
    def connect(self):
         pass

    # 断开连接
    def close(self):
        pass

    # 发送要执行的命令
    def send(self, cmd):
        pass

    
    def transfile_str(self,date,tablename,data,localpath,remotepath):
        """
        /*将类似列表的字符串写进txt，保存在本地，再发送去flag文件夹*/
        """
        self.rasie_err(localpath,remotepath)
        response = data.encode('ISO-8859-1')#
        response = response.decode('utf-8-sig','ignore') #乱码处理
        date=str(date)
        filename=date+'_'+tablename+'.txt'
        remotepathfile=remotepath+''+filename
        localpathfile=localpath+filename
        with open(localpathfile,"w",encoding = " utf-8 ") as f:#将类似列表的字符串写进txt
            f.write(response)
        self.sftp_put(localpathfile,remotepathfile)#再发送去flag文件夹
        
        
    def transfile_df(self,date,tablename,data,localpath,remotepath):
        """
        /*将已形成的df写进csv，保存在本地，再发送去flag文件夹*/
        """
        self.rasie_err(localpath,remotepath)
        date=str(date)
        filename=date+'_'+tablename+'.csv'
        remotepathfile=remotepath+''+filename
        localpathfile=localpath+filename
        data.to_csv(localpathfile,index=False,sep=',',line_terminator="\n")
        self.sftp_put(localpathfile,remotepathfile)
        
    def transfile_json(self,date,tablename,data,localpath,remotepath):
        """
        /*将json数据写进txt，保存在本地，再发送去flag文件夹*/
        """
        self.rasie_err(localpath,remotepath)
        date=str(date)
        filename=date+'_'+tablename+'.txt'
        remotepathfile=remotepath+''+filename
        localpathfile=localpath+filename
        with open(localpathfile,"w",encoding = " utf-8 ") as f:
            f.write(data)
        self.sftp_put(localpathfile,remotepathfile)
        
    def rasie_err(self,localpath,remotepath):
        if localpath[-1] !='/':
            raise IOError ('localpath 地址最后必须要有 /')
        if remotepath[-1] !='/':
            raise IOError ('remotepath 地址最后必须要有 /')
    
    def is_existence(self, filname, path):
        t = paramiko.Transport(sock=(self.ip, 22))
        t.connect(username=self.username, password=self.password)
        sftp = paramiko.SFTPClient.from_transport(t)
        is_existence = True if filname in sftp.listdir(path) else False
        if not is_existence:
            sftp.mkdir(path + filname)
        t.close()


    # get单个文件
    def sftp_get(self, remotefile, localfile):
        t = paramiko.Transport(sock=(self.ip, 22))
        t.connect(username=self.username, password=self.password)
        sftp = paramiko.SFTPClient.from_transport(t)
        sftp.get(remotefile, localfile)
        t.close()

    # put单个文件
    def sftp_put(self, localfile, remotefile):
        t = paramiko.Transport(sock=(self.ip, 22))
        t.connect(username=self.username, password=self.password)
        sftp = paramiko.SFTPClient.from_transport(t)
        sftp.put(localfile, remotefile)
        t.close()
    # ------获取远端linux主机上指定目录及其子目录下的所有文件------
    def __get_all_files_in_remote_dir(self, sftp, remote_dir):
        # 保存所有文件的列表
        all_files = list()

        # 去掉路径字符串最后的字符'/'，如果有的话
        if remote_dir[-1] == '/':
            remote_dir = remote_dir[0:-1]

        # 获取当前指定目录下的所有目录及文件，包含属性值
        files = sftp.listdir_attr(remote_dir)
        for x in files:
            # remote_dir目录中每一个文件或目录的完整路径
            filename = remote_dir + '/' + x.filename
            # 如果是目录，则递归处理该目录，这里用到了stat库中的S_ISDIR方法，与linux中的宏的名字完全一致
            # if S_ISDIR(x.st_mode):
            #     all_files.extend(self.__get_all_files_in_remote_dir(sftp, filename))
            # else:
            all_files.append(filename)
        return all_files
    
    def sftp_get_dir(self, remote_dir, local_dir):
        t = paramiko.Transport(sock=(self.ip, 22))
        t.connect(username=self.username, password=self.password)
        sftp = paramiko.SFTPClient.from_transport(t)

        # 获取远端linux主机上指定目录及其子目录下的所有文件
        all_files = self.__get_all_files_in_remote_dir(sftp, remote_dir)
        # 依次get每一个文件
        for x in all_files:
            filename = x.split('/')[-1]
            local_filename = os.path.join(local_dir, filename)
            print (f'Get文件{filename}传输中...' )
            sftp.get(x, local_filename)

    # ------获取本地指定目录及其子目录下的所有文件------
    def __get_all_files_in_local_dir(self, local_dir):
        # 保存所有文件的列表
        all_files = list()
    
        # 获取当前指定目录下的所有目录及文件，包含属性值
        files = os.listdir(local_dir)
        for x in files:
            # local_dir目录中每一个文件或目录的完整路径
            filename = os.path.join(local_dir, x)
            # 如果是目录，则递归处理该目录
            if os.path.isdir(x):
                all_files.extend(self.__get_all_files_in_local_dir(filename))
            else:
                all_files.append(filename)
        return all_files
    
    def sftp_put_dir(self, local_dir, remote_dir):
        t = paramiko.Transport(sock=(self.ip, 22))
        t.connect(username=self.username, password=self.password)
        sftp = paramiko.SFTPClient.from_transport(t)
    
        # 去掉路径字符穿最后的字符'/'，如果有的话
        if remote_dir[-1] == '/':
            remote_dir = remote_dir[0:-1]
    
        # 获取本地指定目录及其子目录下的所有文件
        all_files = self.__get_all_files_in_local_dir(local_dir)
        # 依次put每一个文件
        for x in all_files:
            filename = os.path.split(x)[-1]
            remote_filename = remote_dir + '/' + filename
            print (f'Put文件{filename}传输中...' )
            sftp.put(x, remote_filename)