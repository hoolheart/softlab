# 工程协作指南

本文件适用于整个仓库。修改前先阅读相关模块及其调用方，以现有实现和
`pyproject.toml` 为准；以下内容描述当前工程，不代表尚未实现的能力。

## 工程概览

softlab 是受 QCoDeS 启发的软件定义实验室 Python 工具库，使用 setuptools
构建，不是独立 Web 应用。源码直接位于 `softlab/`，按“五行”组织：

| 目录 | 职责与主要入口 |
| --- | --- |
| `softlab/jin/` | 通用工具：`validator` 校验器、`misc` 属性委托与数据辅助、`sp` 信号/波形/窗函数/IQ 调制、`vis` 数据绘图；`dp` 目前为占位模块。 |
| `softlab/mu/` | 应用与服务层；现有功能集中在 `notebooks` 的交互与文件选择组件，`cli`、`server`、`services` 主要为占位模块。 |
| `softlab/shui/` | 数据与配置管理：`DatabaseBackend`、`data` 中的 `DataChart`/`DataRecord`/`DataGroup` 及 HDF5、SQLite 后端；`profile` 中的内存和 JSON 配置后端。 |
| `softlab/huo/` | 基于 asyncio 的调度器与流程：`Process`、串行/并行/分支/扫描组合，以及 `count`、`scan`、`grid_scan` 实验流程。 |
| `softlab/tu/` | 基础抽象：`station` 中的参数、设备、工作站和 VISA 接口；`theory` 中的映射与理论模型。 |
| `tests/` | `unittest` 兼容性回归测试、手动验证 Notebook，以及 VISA 仿真配置 `visa_sim.yaml`。 |

`README.rst` 说明整体理念；API 细节主要在源码 docstring 中。
版本来源为 `softlab/VERSION.txt`，由 `softlab/_version.py` 读取。
各级 `__init__.py` 汇集公共 API，修改导出时注意导入顺序和循环依赖。

## 环境与常用命令

从仓库根目录执行。本地 `.venv` 是 Conda 创建的 Python 3.13 环境：

```sh
conda activate "$PWD/.venv"
python -m pip install -e .
python -c "import softlab; print(softlab.__version__)"
```

- 运行仓库中的 Notebook 时使用 `python -m pip install -e '.[notebooks]'`，
  可选依赖组包含 JupyterLab、`nest_asyncio` 和 `pyvisa-sim`。
- 依赖由 `pyproject.toml` 声明，包括 NumPy、SciPy、pandas、Matplotlib、
  Plotly、ipywidgets、PyVISA、h5py 等；目前没有依赖锁文件和开发依赖组。
- 元数据声明 Python >= 3.9，Black 目标为 Python 3.9；本地使用 Python
  3.13 做兼容性验证。其他版本仍需各自运行测试，不要仅凭元数据宣称已验证。
- 顶层 `import softlab` 会导入多个子模块；缺少科学计算或 Notebook 相关
  依赖时，即使只使用一个模块，也可能导入失败。
- 构建分发包时，先安装构建工具，再运行 `python -m build`：
  `python -m pip install build`。`setup.py` 仅为 setuptools 薄入口。

## 修改约定

- 保持“五行”职责划分，优先扩展现有抽象；不要将实验流程、仪器驱动和
  数据持久化逻辑集中到单一模块。
- 公共类使用 PascalCase，函数、属性与模块使用 snake_case。新增或修改
  API 应保留类型注解，并按周边风格编写英文 docstring，说明参数、返回值、
  异常与副作用。
- 格式配置位于 `pyproject.toml`：Black 行宽 80、目标 `py39`，isort 使用
  Black profile。工具未列为项目依赖；需要时单独安装，仅处理本次修改文件，
  避免全仓格式化。
- 新增公共 API 时检查所属包的 `__init__.py`；避免破坏现有导入路径。
- 校验器的 `validate()` 通过抛出异常表示失败；不要随意改成布尔返回值。
  参数修改需维护校验、编码/解码、读写权限与 hook 的执行语义。
- 数据后端应沿用连接状态、错误记录和 `*_impl` 扩展接口。修改存储格式时
  检查既有数据读取和往返保存，关注 UUID、元数据、列定义与数组类型。
- 调度与流程修改需考虑完成、失败、取消及重复运行；验证后释放调度器、
  数据库连接与 VISA 资源，避免全局默认实例造成测试间状态泄漏。
- 依赖或 Python 支持范围变更应显式修改配置并说明原因，不随普通修复附带升级。

## 验证方式

`tests/test_compatibility.py` 是 `unittest` 回归测试，目前没有仓库内 CI 配置。
不要把 Notebook 存在或模块示例打印成功视为完整测试通过。

按修改范围选择验证，并在交付时记录实际命令、结果和未验证项：

1. Python 修改运行 `python -m unittest discover -s tests -p 'test_*.py'`，
   再运行 `python -m compileall -q softlab` 和上述导入冒烟检查。
2. 行为修复应提供有断言的最小复现或回归测试；纯文档修改检查内容、路径和
   diff 即可。新增自动测试时说明运行命令及所需测试依赖。
3. `tests/test_basic.ipynb` 验证基础导入；`test_common_proc.ipynb` 演示计数、
   扫描与绘图，额外依赖 `nest_asyncio`；`sprike_plotly.ipynb` 为绘图探索示例。
4. `tests/test_visa.ipynb` 使用 PyVISA 仿真，额外需要 `pyvisa-sim`。其代码
   根据当前工作目录读取 `visa_sim.yaml`，执行时工作目录应为 `tests/`，
   并使用已安装本项目的 Python 环境。保留 `@sim` 后端，常规验证使用仿真设备。
5. 如需交互运行 Notebook，安装 `notebooks` 可选依赖组；这些包不属于
   核心运行依赖。
6. 多个源码文件含 `if __name__ == '__main__'` 示例。执行前阅读示例，确认
   文件写入、绘图和设备访问等副作用；从仓库根目录使用 `python -m 模块路径`。

涉及真实仪器的操作必须属于用户明确授权的任务范围。持久化验证使用临时
目录和合成数据，不覆盖实验数据；不要提交数据库、HDF5 文件、构建产物、
虚拟环境或无关 Notebook 输出。已有 `.gitignore` 覆盖多种生成文件。

## 提交消息格式

- 本个人项目仅约定提交消息格式，不要求 Gitflow 分支模型。
- 提交消息采用 Conventional Commits：`<type>(<scope>): <summary>`；
  常用类型有 `feat`、`fix`、`docs`、`test`、`chore`。主题用英文祈使句，
  准确描述本次改动；必要时在正文解释原因、行为变化和验证结果。

## 交付检查

- 检查 `git diff --check` 与 `git status --short`，保留用户已有修改。
- 仅提交任务相关改动；不要顺手重命名五行模块、清理占位包或重写历史示例。
- 交付说明交代改动内容、验证结果，以及缺失依赖、硬件或环境导致的验证限制。
