# 增加了一些直接获取的数据
## spot_em
### 1、record_1d 直接获取东财当日全部日线数据
### 2、record_1d_hfq 直接获取东财当日全部日线后复权数据
### 3、stock_yjbb_em 直接获取东财当日业绩快报保存到数据库
### 其余见说明




# zvtm简单说明
## 1、zvt是一个很好的量化框架，不过文档还不够详细。https://zvtvz.github.io/zvt/#/README 是它的介绍。
## 2、为了解决zvt数据保存在sqlite3不方便使用的问题，对其进行了修改，可以保存到mysql数据库。
## 3、使用方法，直接修改config.json 放到本地用户zvtm_home目录，就可以使用了。
## 4、test 目录的record.py做了几个简单的demo。具体详见上面zvt的说明。
## 5、原有数据可以通过sql3mysql进行转化。


  
### pip 安装出错
 error in demjson setup command: use_2to3 is invalid.
 pip install --upgrade setuptools==57.5.0
 python -m pip install --upgrade pip