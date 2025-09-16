import re
import os
import argparse


def split_sql_file(input_file, output_dir):
    # 确保输出目录存在
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 读取整个 SQL 文件
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 使用正则表达式查找所有 CREATE TABLE 语句的位置
    # 匹配模式: CREATE TABLE 表名
    table_pattern = re.compile(r'CREATE TABLE (?:IF NOT EXISTS )?`?(\w+)`?', re.IGNORECASE)

    # 找到所有 CREATE TABLE 的位置
    table_starts = []
    for match in table_pattern.finditer(content):
        table_starts.append((match.group(1), match.start()))

    # 如果没有找到任何表，直接返回
    if not table_starts:
        print("未找到任何 CREATE TABLE 语句")
        return

    # 添加文件结束位置作为最后一个分割点
    table_starts.append(("_end", len(content)))

    # 提取每个表的内容（从前一个 CREATE TABLE 到当前 CREATE TABLE）
    tables = []

    # 处理第一个 CREATE TABLE 之前的内容
    if table_starts[0][1] > 0:
        preamble = content[:table_starts[0][1]]
        tables.append(('_preamble', preamble))

    # 处理每个表的内容
    for i in range(len(table_starts) - 1):
        table_name = table_starts[i][0]
        start_pos = table_starts[i][1]
        end_pos = table_starts[i + 1][1]

        table_content = content[start_pos:end_pos]
        tables.append((table_name, table_content))

    # 写入单独的文件
    for name, sql in tables:
        if name == '_preamble':
            filename = os.path.join(output_dir, '00_preamble.sql')
        elif name == '_end':
            filename = os.path.join(output_dir, '99_postamble.sql')
        else:
            filename = os.path.join(output_dir, f'{name}.sql')

        with open(filename, 'w', encoding='utf-8') as f:
            f.write(sql)

    print(f"成功拆分 {len(tables) - 2} 个表")  # 减去 preamble 和 postamble
    print(f"输出目录: {output_dir}")


if __name__ == '__main__':
    # parser = argparse.ArgumentParser(description='拆分 SQL 备份文件为每个表单独的文件（包含表结构和所有数据）')
    # parser.add_argument('input_file', help='输入的 SQL 文件路径')
    # parser.add_argument('-o', '--output-dir', default='split_tables', help='输出目录（默认为当前目录下的 split_tables 文件夹）')
    #
    # args = parser.parse_args()
    #
    # split_sql_file(args.input_file, args.output_dir)
    input_file = "F:\\203mysqlback\\eastmoney_finance-2025-09-14-00-00.sql"
    output_dir = "F:\\203mysqlback\\split\\"
    split_sql_file(input_file, output_dir)