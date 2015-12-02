@echo off
REM  The contents of this file are subject to the Mozilla Public License
REM  Version 1.1 (the "License"); you may not use this file except in
REM  compliance with the License. You may obtain a copy of the License
REM  at http://www.mozilla.org/MPL/
REM
REM  Software distributed under the License is distributed on an "AS IS"
REM  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
REM  the License for the specific language governing rights and
REM  limitations under the License.
REM
REM  The Original Code is RabbitMQ.
REM
REM  The Initial Developer of the Original Code is GoPivotal, Inc.
REM  Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
REM

REM setlocal和endlocal命令执行结果是让中间的程序对于系统变量的改变只在程序内起作用，不会影响整个系统级别。

goto start
rem Preserve values that might contain exclamation marks before
rem enabling delayed expansion

REM %0 指本批处理文件 d (driver)指驱动器 p (path) 指路径    %~dp0 --- 指本批处理文件的绝对路径

REM 设置本地为延迟扩展。其实也就是：延迟变量，全称"延迟环境变量扩展",
REM 在cmd执行命令前会对脚本进行预处理，其中有一个过程是变量识别过程，在这个过程中，如果有两个%括起来的如%value%类似这样的变量，
REM 就会对其进行识别，并且查找这个变量对应的值，再而将值替换掉这个变量，这个替换值的过程,就叫做变量扩展，然后再执行命令。


REM 需要扩展的变量用!!引用，不需要的用%%
REM !a! 是变量的意思 正常的应该是 %a%  你这是在启动了延迟变量的情况下就要将%改为!号
REM %%i  应该是for中的语句,通过for设的变量.

REM 如果echo 后面什么也没有
REM 那屏幕上就显示“ECHO 处于打开状态”

:start

setlocal
set TDP0=%~dp0
set STAR=%*
setlocal enabledelayedexpansion

if "!RABBITMQ_USE_LONGNAME!"=="" (
    set RABBITMQ_NAME_TYPE="-sname"
)

if "!RABBITMQ_USE_LONGNAME!"=="true" (
    set RABBITMQ_NAME_TYPE="-name"
)

REM rabbit存储的路径
REM RABBITMQ_BASE="C:\Users\cb1223\AppData\Roaming\RabbitMQ"
if "!RABBITMQ_BASE!"=="" (
    set RABBITMQ_BASE=!APPDATA!\RabbitMQ
)

REM 电脑名字
REM COMPUTERNAME=XXW-PC
if "!COMPUTERNAME!"=="" (
    set COMPUTERNAME=localhost
)

REM 消息队列的节点名
if "!RABBITMQ_NODENAME!"=="" (
    set RABBITMQ_NODENAME=rabbit@!COMPUTERNAME!
)

REM rabbit的ip地址和监听端口号
if "!RABBITMQ_NODE_IP_ADDRESS!"=="" (
	set RABBITMQ_NODE_IP_ADDRESS=127.0.0.1
	set RABBITMQ_NODE_PORT=5672
)

if "!RABBITMQ_NODE_IP_ADDRESS!"=="" (
   if not "!RABBITMQ_NODE_PORT!"=="" (
      set RABBITMQ_NODE_IP_ADDRESS=auto
   )
) else (
   if "!RABBITMQ_NODE_PORT!"=="" (
      set RABBITMQ_NODE_PORT=5672
   )
)

if "!RABBITMQ_DIST_PORT!"=="" (
   if "!RABBITMQ_NODE_PORT!"=="" (
      set RABBITMQ_DIST_PORT=25672
   ) else (
      set /a RABBITMQ_DIST_PORT=20000+!RABBITMQ_NODE_PORT!
   )
)

REM 判断是否安装了Erlang,如果没有则打印出错误的消息
if not exist "!ERLANG_HOME!\bin\erl.exe" (
    echo.
    echo ******************************
    echo ERLANG_HOME not set correctly.
    echo ******************************
    echo.
    echo Please either set ERLANG_HOME to point to your Erlang installation or place the
    echo RabbitMQ server distribution in the Erlang lib folder.
    echo.
    exit /B 1
)

REM 设置rabbit的mnesia数据库存储基础路径
if "!RABBITMQ_MNESIA_BASE!"=="" (
    set RABBITMQ_MNESIA_BASE=!RABBITMQ_BASE!/db
)

REM 设置rabbit日志的存储路径
if "!RABBITMQ_LOG_BASE!"=="" (
    set RABBITMQ_LOG_BASE=!RABBITMQ_BASE!/log
)


REM We save the previous logs in their respective backup
REM Log management (rotation, filtering based of size...) is left as an exercice for the user.

REM 正常日志的路径
set LOGS=!RABBITMQ_LOG_BASE!\!RABBITMQ_NODENAME!.log
REM sasl日志的路径
set SASL_LOGS=!RABBITMQ_LOG_BASE!\!RABBITMQ_NODENAME!-sasl.log

REM End of log management

REM 设置rabbit的mnesia数据库存储路径
if "!RABBITMQ_MNESIA_DIR!"=="" (
    set RABBITMQ_MNESIA_DIR=!RABBITMQ_MNESIA_BASE!/!RABBITMQ_NODENAME!-mnesia
)

REM 设置扩展的目录路径
if "!RABBITMQ_PLUGINS_EXPAND_DIR!"=="" (
    set RABBITMQ_PLUGINS_EXPAND_DIR=!RABBITMQ_MNESIA_BASE!/!RABBITMQ_NODENAME!-plugins-expand
)

if "!RABBITMQ_ENABLED_PLUGINS_FILE!"=="" (
    set RABBITMQ_ENABLED_PLUGINS_FILE=!RABBITMQ_BASE!\enabled_plugins
)

REM 设置rabbit的扩展控件的路径(指本批处理文件的上一级目录的下一级plugins目录)
if "!RABBITMQ_PLUGINS_DIR!"=="" (
    set RABBITMQ_PLUGINS_DIR=!TDP0!..\plugins\
)

REM 设置rabbit ebin的目录路径
set RABBITMQ_EBIN_ROOT=!TDP0!..\ebin

"!ERLANG_HOME!\bin\erl.exe" ^
        -pa "!RABBITMQ_EBIN_ROOT!" ^
        -noinput -hidden ^
        -s rabbit_prelaunch ^
        !RABBITMQ_NAME_TYPE! rabbitmqprelaunch!RANDOM!!TIME:~9! ^
        -extra "!RABBITMQ_NODENAME!"

if ERRORLEVEL 2 (
    rem dist port mentioned in config, do not attempt to set it
) else if ERRORLEVEL 1 (
    exit /B 1
) else (
    set RABBITMQ_DIST_ARG=-kernel inet_dist_listen_min !RABBITMQ_DIST_PORT! -kernel inet_dist_listen_max !RABBITMQ_DIST_PORT!
)

set RABBITMQ_EBIN_PATH="-pa !RABBITMQ_EBIN_ROOT!"

REM 设置config配置文件的路径
if "!RABBITMQ_CONFIG_FILE!"=="" (
    set RABBITMQ_CONFIG_FILE=!RABBITMQ_BASE!\rabbitmq
)

if exist "!RABBITMQ_CONFIG_FILE!.config" (
    set RABBITMQ_CONFIG_ARG=-config "!RABBITMQ_CONFIG_FILE!"
) else (
    set RABBITMQ_CONFIG_ARG=
)

set RABBITMQ_LISTEN_ARG=
if not "!RABBITMQ_NODE_IP_ADDRESS!"=="" (
   if not "!RABBITMQ_NODE_PORT!"=="" (
      set RABBITMQ_LISTEN_ARG=-rabbit tcp_listeners [{\""!RABBITMQ_NODE_IP_ADDRESS!"\","!RABBITMQ_NODE_PORT!"}]
   )
)

set RABBITMQ_START_RABBIT=
if "!RABBITMQ_NODE_ONLY!"=="" (
    set RABBITMQ_START_RABBIT=-s rabbit boot
)

"!ERLANG_HOME!\bin\erl.exe" ^
-pa "!RABBITMQ_EBIN_ROOT!" ^
-boot start_sasl ^
!RABBITMQ_START_RABBIT! ^
!RABBITMQ_CONFIG_ARG! ^
!RABBITMQ_NAME_TYPE! !RABBITMQ_NODENAME! ^
+W w ^
+A30 ^
+P 1048576 ^
-kernel inet_default_connect_options "[{nodelay, true}]" ^
!RABBITMQ_LISTEN_ARG! ^
!RABBITMQ_SERVER_ERL_ARGS! ^
!RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS! ^
-sasl errlog_type error ^
-sasl sasl_error_logger false ^
-rabbit error_logger {file,\""!LOGS:\=/!"\"} ^
-rabbit sasl_error_logger {file,\""!SASL_LOGS:\=/!"\"} ^
-rabbit enabled_plugins_file \""!RABBITMQ_ENABLED_PLUGINS_FILE:\=/!"\" ^
-rabbit plugins_dir \""!RABBITMQ_PLUGINS_DIR:\=/!"\" ^
-rabbit plugins_expand_dir \""!RABBITMQ_PLUGINS_EXPAND_DIR:\=/!"\" ^
-os_mon start_cpu_sup false ^
-os_mon start_disksup false ^
-os_mon start_memsup false ^
-mnesia dir \""!RABBITMQ_MNESIA_DIR:\=/!"\" ^
!RABBITMQ_SERVER_START_ARGS! ^
!RABBITMQ_DIST_ARG! ^
!STAR!

endlocal
endlocal
pause
