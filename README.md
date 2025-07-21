  # dmDjango

该仓库主要提供了支持通过Django连接达梦数据库的方言包

# 主要功能

支持Django的基本功能在Django连接达梦数据库中的实现，支持的功能包括表的映射、类型的映射、对于数据库内进行操作、对于返回信息的展示等，支持Django各个对应版本

# 使用方法

### 1、安装
源码安装：
  在setup.py文件夹下，在较高的pip版本支持下，可以通过 ``` pip install . ``` 进行安装，如果提示报错，请通过 ``` python setup.py install ``` 进行安装
使用pip命令安装：
  可以通过 ``` pip install dmDjango ``` 进行下载安装，需要注意的是由于Django有版本区分，如果当前版本不适配，请指定版本下载，如 ```pip install dmDjango==2.0.3 ``` 进行下载2.0.3版本的dmDjango，具体的dmDjango与Django版本对应关系请参照ChangeLogs.md文件描述

### 2、使用
直接通过使用Django连接达梦数据库可以直接使用dmDjango，在Django项目中需要在settings.py中配置以下连接属性
例如：
```
DATABASES = {

    'default': {

        'ENGINE': 'dmDjango',

        'NAME': 'DAMENG',

        'USER': 'SYSDBA',

        'PASSWORD': '******',

        'HOST': 'localhost',

        'PORT': '5236',

        'OPTIONS': {'local_code': 1, 'connection_timeout': 5},
    }

}

```
