# MiniSQL


�?框架参考CMU-15445 BusTub框架进�?�改写，在保留了缓冲池、索引、�?�录模块的一些核心�?��?�理念的基�?�上，做了一些修改和扩展，使之兼容于原MiniSQL实验指�?�的要求�?
以下列出了改�?/扩展较大的几�?地方�?
- 对Disk Manager模块进�?�改�?，扩展了位图页、�?�盘文件元数�?页用于支持持久化数据页分配回收状态；
- 在Record Manager, Index Manager, Catalog Manager等模块中，通过对内存�?�象的序列化和反序列化来持久化数�?�?
- 对Record Manager层的一些数�?结构（`Row`、`Field`、`Schema`、`Column`等）和实现进行重构；
- 对Catalog Manager层进行重构，�?持持久化并为Executor层提供接口调�?;
- 扩展了Parser层，Parser层支持输出�??法树供Executor层调�?�?

此�?�还涉及到很多零碎的改动，包�?源代码中部分模块代码的调整，测试代码的修改，性能上的优化等，在�?�不做赘述�?


注意：为了避免代码抄�?，�?�不要将�?己的代码发布到任何公共平台中�?

### 编译&开发环�?
- Apple clang version: 11.0+ (MacOS)，使用`gcc --version`和`g++ --version`查看
- gcc & g++ : 8.0+ (Linux)，使用`gcc --version`和`g++ --version`查看
- cmake: 3.20+ (Both)，使用`cmake --version`查看
- gdb: 7.0+ (Optional)，使用`gdb --version`查看
- flex & bison (暂时不需要安装，但�?�果需要�?�SQL编译器的�?法进行修改，需要安装）
- llvm-symbolizer (暂时不需要安�?)
    - in mac os `brew install llvm`, then set path and env variables.
    - in centos `yum install devtoolset-8-libasan-devel libasan`
    - https://www.jetbrains.com/help/clion/google-sanitizers.html#AsanChapter
    - https://www.jianshu.com/p/e4cbcd764783

### 构建
#### Windows
�?前�?�代码暂不支持在Windows平台上的编译。但在Win10及以上的系统�?，可以通过安�?�WSL（Windows的Linux子系统）来进�?
开发和构建。WSL请选择Ubuntu子系统（推荐Ubuntu20及以上）。�?�果你使用Clion作为IDE，可以在Clion�?配置WSL从而进行调试，具体请参�?
[Clion with WSL](https://blog.jetbrains.com/clion/2018/01/clion-and-linux-toolchain-on-windows-are-now-friends/)

#### MacOS & Linux & WSL
基本构建命令
```bash
mkdir build
cd build
cmake ..
make -j
```
若不涉及到`CMakeLists`相关文件的变动且没有新�?�或删除`.cpp`代码（通俗来�?�，就是�?�?对现有代码做了修改）
则无需重新执�?�`cmake..`命令，直接执行`make -j`编译即可�?

默�?�以`debug`模式进�?�编译，如果你需要使用`release`模式进�?�编译：
```bash
cmake -DCMAKE_BUILD_TYPE=Release ..
```

### 测试
在构建后，默认会在`build/test`�?录下生成`minisql_test`的可执�?�文件，通过`./minisql_test`即可运�?�所有测试�?

如果需要运行单�?测试，例如，想�?�运行`lru_replacer_test.cpp`对应的测试文件，�?以通过`make lru_replacer_test`
命令进�?�构建�?
