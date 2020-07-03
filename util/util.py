import csv


# 字典类型输出到文件
def print_to_csv(filename, data, headers):
    with open(filename, 'w', newline='')as f:
        f_csv = csv.DictWriter(f, headers)
        f_csv.writeheader()
        f_csv.writerows(data)