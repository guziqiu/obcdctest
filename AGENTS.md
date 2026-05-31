# 项目指令

请遵循全局 Codex 指令 `/Users/pupu/.codex/RTK.md`：执行 shell 命令时统一加上 `rtk` 前缀。

## 编译验证

这个项目依赖容器内挂载的 OceanBase 源码树和工具链，因此必须在 CentOS 8.3 容器内编译验证。

使用当前运行中的 Docker 容器：

```bash
docker exec centos8.3 bash -lc 'cd /usr/local/code/obcdctest/build && rm -rf ./* && cmake ../ && make -j4'
```

项目源码在容器内的挂载路径是：

```text
/usr/local/code/obcdctest
```

如果在本地 macOS 环境因为缺少 `/usr/local/code/oceanbase` 头文件而编译失败，不要把它当成项目真实编译失败。权威编译结果以容器内上述命令为准。

都按照中文输出文档
