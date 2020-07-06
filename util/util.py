import csv


FILE_DIRECTORY = 'D:\\code\\Gephi\\data\\'


# 字典类型输出到文件
def print_dist_lines_to_csv(filename, data, headers, type):
    filename = FILE_DIRECTORY + filename
    with open(filename, type, newline='')as f:
        f_csv = csv.DictWriter(f, headers)
        f_csv.writeheader()
        f_csv.writerows(data)


# 按行输出到文件
def print_list_row_to_csv(filename, data, type):
    filename = FILE_DIRECTORY + filename
    with open(filename, type, newline='')as f:
        f_csv = csv.writer(f)
        f_csv.writerow(data)


