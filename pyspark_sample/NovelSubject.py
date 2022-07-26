from util import JsonUtil


class NovelSubject:

    __slots__ = ['subject', 'detail_url']

    def __init__(self, subject: str, detail_url: str):
        self.subject = subject
        self.detail_url = detail_url

    # 해당 class를 json 문자열로 변환하기 위한 dict를 반환하는 함수
    def json_dict(self) -> dict:
        return {
            'subject': self.subject,
            'detail_url': self.detail_url
        }

    def __str__(self):
        return JsonUtil.json_dump(self.json_dict())