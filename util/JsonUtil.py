import json

def default(obj):
    if hasattr(obj,'json_dict'):
        return obj.json_dict()
    else:
        return json.JSONEncoder.default(obj)

def json_dump(param_obj:object) -> str:
    return json.dumps(param_obj, ensure_ascii=False, default=default)