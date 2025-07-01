# 上证信息披露考评是一个pdf 只好读取并保存到csv
import pdfplumber
import pandas as pd
import os
from tqdm import tqdm


def extract_pdf_data(pdf_path):
    """
    从PDF文档中提取上市公司信息披露评价数据
    :param pdf_path: PDF文件路径
    :return: 包含所有数据的DataFrame
    """
    # 检查文件是否存在
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF文件不存在: {pdf_path}")

    print(f"开始解析PDF文件: {os.path.basename(pdf_path)}")

    all_data = []
    total_pages = 0
    processed_pages = 0

    try:
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            print(f"PDF共 {total_pages} 页")

            # 创建进度条
            progress_bar = tqdm(total=total_pages, desc="处理页面")

            # 遍历每一页
            for page in pdf.pages:
                try:
                    # 提取当前页的表格
                    tables = page.extract_tables()

                    # 处理每个表格
                    for table in tables:
                        # 跳过空表
                        if not table or len(table) < 2:
                            continue

                        # 处理每一行数据
                        for row_idx, row in enumerate(table):
                            # 跳过表头行
                            if row_idx == 0 and ("序号" in row or "证券代码" in row):
                                continue

                            # 检查行是否有效
                            if len(row) < 11:
                                continue

                            # 提取关键字段
                            serial = row[1]  # 序号
                            code = row[4]  # 证券代码
                            name = row[7]  # 证券简称

                            # 评价结果可能在位置9或10
                            rating = row[9] if row[9] in ['A', 'B', 'C', 'D'] else row[10]

                            # 确保所有字段都有值
                            if serial and code and name and rating:
                                all_data.append([serial, code, name, rating])

                except Exception as e:
                    print(f"处理第 {processed_pages+1} 页时出错: {str(e)}")

                processed_pages += 1
                progress_bar.update(1)

            progress_bar.close()

    except Exception as e:
        print(f"解析PDF时发生错误: {str(e)}")
        return None

    # 创建DataFrame
    if not all_data:
        print("未提取到任何数据")
        return None

    try:
        # 创建DataFrame
        df = pd.DataFrame(all_data, columns=['序号', '证券代码', '证券简称', '评价结果'])

        # 数据清洗
        df['序号'] = pd.to_numeric(df['序号'], errors='coerce').fillna(0).astype(int)

        # 处理证券代码
        df['证券代码'] = df['证券代码'].apply(
            lambda x: str(x).strip().zfill(6) if str(x).isdigit() else str(x)
        )

        # 处理证券简称
        df['证券简称'] = df['证券简称'].str.strip()

        # 处理评价结果
        df['评价结果'] = df['评价结果'].str.strip().str.upper()

        # 删除空值
        df = df.dropna(subset=['证券代码', '证券简称'])

        # 删除重复项
        df = df.drop_duplicates(subset=['证券代码'])

        print(f"成功提取 {len(df)} 条记录")
        return df

    except Exception as e:
        print(f"创建DataFrame时出错: {str(e)}")
        return None


def save_to_csv(df, output_path):
    """将DataFrame保存为CSV文件"""
    if df is None or df.empty:
        print("无数据可保存")
        return False

    try:
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"数据已保存到: {output_path}")
        return True
    except Exception as e:
        print(f"保存CSV文件时出错: {str(e)}")
        return False


def main():
    """主函数"""
    # 配置输入输出路径
    pdf_path = "F:\\203\\book\\股票\\附件：沪市上市公司信息披露工作评价结果（2023-2024）.pdf"  # 替换为您的PDF文件路径
    output_csv = "disclosure_evaluation_results.csv"

    # 提取数据
    df = extract_pdf_data(pdf_path)

    if df is not None:
        # 保存数据
        save_to_csv(df, output_csv)

        # 打印前10条记录
        print("\n数据预览 (前10条):")
        print(df.head(10))

        # 打印统计信息
        print("\n评价结果分布:")
        print(df['评价结果'].value_counts())

        # 打印总记录数
        print(f"\n总记录数: {len(df)}")
    else:
        print("未能提取数据，请检查PDF文件格式")


if __name__ == "__main__":
    # 安装必要库（如果尚未安装）
    try:
        import pdfplumber
    except ImportError:
        print("正在安装依赖库...")
        import subprocess

        subprocess.run(["pip", "install", "pdfplumber", "pandas", "tqdm"])

    main()
