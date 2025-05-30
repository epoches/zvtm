# # 上证信息披露考评是一个pdf 只好读取并保存
# import pdfplumber
# import pandas as pd
# import os
#
# def extract_pdf_table_to_df(pdf_path, show_progress=False):
#     """增强版PDF表格提取函数，带进度显示"""
#     from tqdm import tqdm
#
#     if not os.path.exists(pdf_path):
#         raise FileNotFoundError(f"PDF文件不存在: {pdf_path}")
#
#     all_data = []
#     total_pages = 0
#     processed_pages = 0
#
#     with pdfplumber.open(pdf_path) as pdf:
#         total_pages = len(pdf.pages)
#         progress = tqdm(total=total_pages, desc="处理PDF页面", disable=not show_progress)
#
#         for page in pdf.pages:
#             try:
#                 tables = page.extract_tables()
#
#                 for table in tables:
#                     for row_idx, row in enumerate(table):
#                         if len(row) == 4:
#                             # 跳过表头行（检查第一行或第二行）
#                             if row_idx < 2 and (row[0] == "序号" or row[1] == "证券代码"):
#                                 continue
#
#                             # 清理数据：去除多余空格和空值
#                             clean_row = [str(item).strip() if item is not None else "" for item in row]
#                             all_data.append(clean_row)
#             except Exception as e:
#                 print(f"处理第 {processed_pages+1} 页时出错: {e}")
#
#             processed_pages += 1
#             progress.update(1)
#
#         progress.close()
#
#     # 创建DataFrame
#     df = pd.DataFrame(all_data, columns=['序号', '证券代码', '证券简称', '评价结果'])
#
#     # 转换序号为数值类型
#     df['序号'] = pd.to_numeric(df['序号'], errors='coerce')
#
#     # 删除可能的空行
#     df = df.dropna(subset=['证券代码'])
#
#     print(f"成功处理 {processed_pages}/{total_pages} 页，提取 {len(df)} 条记录")
#     return df
#
# # 使用示例
# if __name__ == "__main__":
#     # 假设pdf_text是PDF文本内容
#     file_path = r"F:\203\book\股票\附件：沪市上市公司信息披露工作评价结果（2023-2024）.pdf"
#
#     df = extract_pdf_table_to_df(file_path)
#
#     # 查看数据
#     print(df.head())
#

import pandas as pd
import re


def extract_pdf_table_from_text(pdf_text):
    """
    直接从PDF文本内容中提取表格数据

    参数:
    pdf_text (str): PDF文本内容

    返回:
    pd.DataFrame: 包含表格数据的DataFrame
    """
    # 初始化数据存储列表
    data = []

    # 使用正则表达式匹配表格行
    table_pattern = re.compile(r'^\|\s*(\d+)\s*\|\s*(\d{6})\s*\|\s*([^\|]+?)\s*\|\s*([A-D])\s*\|$')

    # 按行处理文本
    lines = pdf_text.split('\n')
    for line in lines:
        # 跳过非表格行
        if not line.strip().startswith('|'):
            continue

        # 尝试匹配表格行模式
        match = table_pattern.match(line.strip())
        if match:
            # 提取并清理数据
            row = [
                match.group(1).strip(),  # 序号
                match.group(2).strip(),  # 证券代码
                match.group(3).strip(),  # 证券简称
                match.group(4).strip()  # 评价结果
            ]
            data.append(row)

    # 创建DataFrame
    df = pd.DataFrame(data, columns=['序号', '证券代码', '证券简称', '评价结果'])

    # 转换序号为数值类型
    df['序号'] = pd.to_numeric(df['序号'], errors='coerce')

    return df


# 使用示例
if __name__ == "__main__":
    # 从文件读取PDF文本内容
    with open(r"F:\203\book\股票\附件：沪市上市公司信息披露工作评价结果（2023-2024）.pdf", 'r', encoding='utf-8') as f:
        pdf_text = f.read()

    # 提取数据
    df = extract_pdf_table_from_text(pdf_text)

    # 保存到CSV文件
    df.to_csv('disclosure_ratings.csv', index=False, encoding='utf-8-sig')

    # 打印结果
    print(f"成功提取 {len(df)} 条记录")
    print(df.head())