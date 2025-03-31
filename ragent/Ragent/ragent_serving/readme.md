# RagentServing本地使用教程

ragent serving依赖antflow，可以先安装antflow.

## 安装antflow
如果也会修改antflow代码，可以如下：
```
git clone git@code.alipay.com:shenglongshuai.sls/src.git
git fetch origin master
git checkout -b master origin/master

pip install -e ./backend
```

如果不修改antflow，可以直接用pip 命令安装：

```
pip install antflow==0.8.5.dev0 --index=https://artifacts.antgroup-inc.cn/artifact/repositories/simple/ 
```

## 本地安装ragent
如果安装ragent已经安装过了就不用安装
```
cd X/ray_common_libs/Ragent
pip install -e ./
```

## 执行测试demo&example

test_agent_serving_base这个demo是跑的antflow 的一个简单Rag Agent.
flow 展示如下语雀链接中的第一个：
https://yuque.antfin.com/aitech/kf9i3g/vhd6ac9eos2ilb04?singleDoc# 《langflow 实验样例》

```
cd X/ray_common_libs/Ragent 
pytest -sv ragent_serving/tests/test_agent_serving_base.py
```