import chardet
import os
from datetime import datetime
import json


current_directory = "D:/GPT/Ptn/Files&JSON"
root_dir = current_directory + "/project_root/" # наш корень по заданию

# Задание 1.1
path_task = [root_dir, "/data", "/data/raw", "/data/processed", "/logs", "/backups", "/output"]
# Создание или несоздание директорий, если он созданы
os.mkdir(root_dir) if not os.path.exists(root_dir) else None
[os.mkdir(root_dir + elem) for elem in path_task[1:] if not os.path.exists(root_dir + elem)]

print(f"Печать директории: {root_dir}")
for root, dirs, files in os.walk(root_dir):
        for dir in dirs:
            print(os.path.relpath(os.path.join(root, dir), root_dir))
            
# Задание 1.2
temp_dir = root_dir + "/data/raw/"
# Создаем 2 файла с разными кодировками и на разных языках одни текст внутри
with open(temp_dir + "file1.txt", "w", encoding="utf-8") as file1:
    example_string = """ Всем привет, heLLo!
    Пробный TeXt для понимания работы с coDec 
    Текст закончен. Пока.
    1254 eNd.
    """
    file1.write(example_string)
    
with open(temp_dir + "file2.txt", "w", encoding="ISO-8859-1") as file2: #ISO-8859-1
    example_string = example_string.encode("ISO-8859-1", errors="ignore").decode("ISO-8859-1")
    file2.write(example_string)

# Создаем лог файл, проверяем директории и файлы внутри
temp_dir = root_dir + "/logs/"
print()
with open(temp_dir + "logs.txt", "w", encoding="utf-8") as logs:
    for root, dirs, files in os.walk(root_dir):
        for dir in dirs:
            name_dir = os.path.normpath(os.path.join(root, dir))
            time_create = datetime.fromtimestamp(os.path.getctime(name_dir))
            logs.write(f"Создали директорию {name_dir}. Дата и время создания: { time_create}\n")
        for fl in files:
            name_dir = os.path.normpath(os.path.join(root, fl))
            time_create = datetime.fromtimestamp(os.path.getmtime(name_dir))
            logs.write(f"Создали файл {name_dir}. Дата и время создания: { time_create}\n")
            
# 2.1
temp_dir = root_dir + "/data/raw/" # Определяем директории куда писать файлы
temp_dir_w = root_dir + "/data/processed/"

for root, dirs, files in os.walk(temp_dir):
        for fl in files:
            file_name_w = temp_dir_w + os.path.basename(fl).split(".")[0] + "_processed.txt" 
            file_name_r = os.path.normpath(os.path.join(root, fl))
            with open(file_name_r, 'rb') as file: # Определяем кодировку 
                rawdata = file.readline()
                result = chardet.detect(rawdata)
                encod =  result['encoding']          
            with open(file_name_r, "r", encoding=encod) as file_r, open(file_name_w, "w", encoding=encod) as file_w:
                for line in file_r.readlines(): # Меняем буквы и пишем в новые файлы 
                    file_w.write(line.swapcase())
                    
# Задание 2.2
# temp_dir_w = /data/processed/" Это директория, где лежат обрабатываемые файлы
data_ser = []
for root, dirs, files in os.walk(temp_dir_w):
        for fl in files:
            file_name_r_new = os.path.normpath(os.path.join(root, fl))
            file_name_r_old = file_name_r_new.replace("_processed","") # c путями, именами старых и новых файлов определяемся
            file_name_r_old = file_name_r_old.replace("processed","raw")
            
            with open(file_name_r_new, "r", encoding="utf-8") as file_r:
                text_file_new = file_r.read() # текст старого забираем
            size_new = os.path.getsize(file_name_r_new)
            datetime_new = datetime.fromtimestamp(os.path.getmtime(file_name_r_new)).strftime("%d.%m.%y")
             
            with open(file_name_r_old, "r", encoding="utf-8") as file_r:
                text_file_old = file_r.read() # текст старого забираем
            
            temp_dict = {"file_name":fl, "text_source":text_file_old, "text_now_":text_file_new, "size":size_new, "dt":datetime_new}
            # print(json.dumps(temp_dict, indent=4, ensure_ascii=False))           
            data_ser.append(temp_dict)
            

with open(root_dir + "/output/processed_data.json", 'w', encoding="utf-8") as file:
    json.dump(data_ser, file, ensure_ascii=False, indent=10)
    
with open(root_dir + "/output/processed_data.json", 'r', encoding="utf-8") as file:
    data_ser_check = json.load(file)
# print(*[json.dumps(c, indent = 4) for c in data_ser_check])

# 3.1 Архивация
from zipfile import ZipFile

filename_zip = current_directory + "/buckup_" + datetime.now().strftime("%Y%m%d") + ".zip"
list_file_minicheck = []

with ZipFile(filename_zip, "w") as zip_file:
    os.chdir(root_dir)
    for root, dirs, files in os.walk(root_dir + "/data/"):
        for fl in files:
            file_for_zip = os.path.normpath(os.path.join(root, fl))
            # file_for_zip = os.path.basename(os.path.join(root, fl))
            # Для выполнения 3.2 заполним словарь по файлам в data, чтобы потом сравнивать
            list_file_minicheck.append({"filename":file_for_zip, "size":os.path.getsize(file_for_zip)})
            zip_file.write(file_for_zip, os.path.relpath(file_for_zip, root_dir + "/data/" ))

print("Архив \n", *zip_file.namelist(), sep='\n')

import shutil
shutil.copy(filename_zip, filename_zip.replace(current_directory, current_directory+"/project_root/backups"))

# 3.2 Разархивирование и проверка
list_file_minicheck_new = []
os.chdir(root_dir + "/data/")
with ZipFile(filename_zip, "r") as zip_file:
     zip_file.extractall()
     for root, dirs, files in os.walk(root_dir + "/data/"):
        for fl in files:
            file_for_zip = os.path.normpath(os.path.join(root, fl))
            # Для выполнения 3.2 заполним словарь по файлам в data, само содержимое не вытаскиваю
            list_file_minicheck_new.append({"filename":file_for_zip, "size":os.path.getsize(file_for_zip)})
 
if sorted(list_file_minicheck, key = lambda x: x["filename"]) == sorted(list_file_minicheck_new, key = lambda x: x["filename"]):           
    print("Все файлы восстановлены в соответствующие директории, и их размер не зменился\n")
else:
    print("Сбой в распаковке\n")     
    
# 4.1
class FileInfo():
    def __init__(self, file_name, file_path, file_size, file_date) -> None:
        self.file_name = file_name
        self.file_path = file_path
        self.file_size = file_size
        self.file_date = file_date
    
    # Понадобится для проверки десериализации объектов, сравнивать будем списки с объектами
    def __eq__(self, value: object) -> bool:
        return all(map(lambda x: getattr(self, x) == getattr(value, x), self.__dict__))

# Создаем список объектов FileInfo для сер. и десер.
list_file_info = []
for root, dirs, files in os.walk(root_dir + "/data/processed"):
        for fl in files:
            file_for_obj = os.path.normpath(os.path.join(root, fl))
            file_size = os.path.getsize(file_for_zip)
            file_date = datetime.fromtimestamp(os.path.getmtime(file_for_obj)).strftime("%d.%m.%y")
            list_file_info.append(FileInfo(fl, file_for_obj, file_size, file_date))



# Функция для сериализации объектов Employee в JSON
def employee_to_dict(fi:FileInfo):
    return {"file_name": fi.file_name, "file_path": fi.file_path, 
            "file_size": fi.file_size, "file_date":fi.file_date}

# Сериализация списка объектов 
json_file_info = [employee_to_dict(fi) for fi in list_file_info]
with open(root_dir + "/output/obj_data.json", 'w', encoding="utf-8") as file:
    json.dump(json_file_info, file)
    
# Проверка десериализации объектов
with open(root_dir + "/output/obj_data.json", 'r', encoding="utf-8") as file:
    json_file_info_new = json.load(file)
    
# Преобразование JSON обратно в объекты FileInfo
list_file_info_new = [FileInfo(**fi) for fi in json_file_info_new]
if sorted(list_file_info_new, key = lambda x: x.file_name) == sorted(list_file_info, key = lambda x: x.file_name):           
    print("Все объекты десериализованы правильно\n")
else:
    print("Сбой в дср.\n")   
    
    
# 4.2 Схемы   
from jsonschema import validate, ValidationError

schema = {
    "type": "object",
    "properties": {
        "file_name": {"type": "string"},
        "file_path": {"type": "string"},
        "file_size": {"type": "integer"},
        "file_date": {"type": "string"}
    },
    "required": ["file_name", "file_path", "file_size", "file_date"]
}
result = True

for sc in json_file_info_new:
    try:  
        validate(instance=sc, schema=schema) 
    except ValidationError as e:
        print(f"Ошибка валидации: {e}")
        result = False
if result:
    print("Валиден файл json")
    
