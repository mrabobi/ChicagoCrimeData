import json

json_template = json.loads(open("json_template.txt", "r").read())


def getAllData():
    return json_template


def getDatByYear(year):
    result = []
    for item in json_template:
        if item['year'] == str(year):
            result.append(item)

    if len(result) != 0:
        print(str(result))
        return json.dumps(result)

    return False


def getData(category, value):
    result = []
    for item in json_template:
        if category in list(item.keys()):
            if str(item[category]) == str(value):
                result.append(item)

    if len(result) != 0:
        print(str(result))
        return json.dumps(result)

    return False
