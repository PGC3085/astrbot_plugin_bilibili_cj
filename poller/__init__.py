"""轮询器子包：live / dynamic / collection 三套订阅轮询器。

各轮询器只依赖 repository 接口与 db / push 注入，不直接接触 B 站 SDK
（SDK 仅在 repository/bili.py 内被引用）。调度与生命周期由 T10 scheduler
与 T14 main.py 负责。
"""
