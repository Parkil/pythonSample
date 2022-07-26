from NovelSubject import NovelSubject
from typing import List

novel_subject_list: List[NovelSubject] = [NovelSubject('소설제목1', 'url1'), NovelSubject('소설제목2', 'url2')]
json_list = list(map(lambda row: row.json_dict(), novel_subject_list))

print(json_list)
