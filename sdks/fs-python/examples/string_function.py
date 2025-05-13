import asyncio
import sys
import os

from fs_sdk import FSFunction

async def string_process_function(request_data: dict) -> dict:
    """
    处理字符串的函数，在字符串后面添加感叹号
    
    Args:
        request_data (dict): 请求数据，需要包含 'text' 字段
        
    Returns:
        dict: 处理后的字符串
    """
    # 获取输入文本
    text = request_data.get('text', '')
    
    # 在文本后面添加感叹号
    result = f"{text}!"

    print(f"Result: {result}")
    
    return {"result": result}

async def main():
    # 初始化函数
    function = FSFunction(
        process_funcs={
            'string': string_process_function
        }
    )

    try:
        print("启动字符串处理函数服务...")
        await function.start()
    except asyncio.CancelledError:
        print("\n正在优雅地关闭服务...")
    except Exception as e:
        print(f"\n发生错误: {e}")
    finally:
        await function.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n服务已停止") 