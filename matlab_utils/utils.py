import platform
from matplotlib import rcParams
from matplotlib import font_manager

def set_chinese_font():
    system = platform.system()  # 获取操作系统名称
    if system == "Windows":
        # Windows 使用 SimHei
        rcParams['font.sans-serif'] = ['SimHei']
    elif system == "Darwin":
        # macOS 使用 PingFang SC
        rcParams['font.sans-serif'] = ['Heiti TC']
    else:
        # 其他系统提示用户自行安装字体
        raise EnvironmentError("Unsupported OS. Please install a suitable Chinese font.")

    # 解决负号显示问题
    rcParams['axes.unicode_minus'] = False