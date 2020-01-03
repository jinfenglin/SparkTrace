import os
import pandas as pd


def build_package_index():
    vista_code = "G:\Download\VistA-M-master\Packages"
    index = {}
    packages = os.listdir(vista_code)
    for pk in packages:
        pk_path = os.path.join(vista_code, pk)
        for dirName, subdirList, fileList in os.walk(pk_path):
            for fname in fileList:
                index[fname] = pk
    return index, packages


def readReqTopicIndex():
    req_path = "G:\Download\Vista\VistA RequirementsHierarchy.xlsx"
    df = pd.read_excel(req_path, sheet_name="Sheet1")
    req_index = {}
    req_content = {}
    req_ids = df["Regulation ID"]
    topics = df["Sub-section"]
    contents = df["Requirement"]
    for req_id, topic, content in zip(req_ids, topics, contents):
        req_index[req_id] = topic
        req_content[req_id] = content
    return req_index, topics, req_content


def write_pk_req(pk_req: dict, req_content):
    res = {}
    requirement = None
    for package in pk_req:
        res[package] = list(pk_req[package].values())
        if requirement is None:
            requirement = list(pk_req[package].keys())
    res["req_id"] = requirement
    res["req_content"] = [req_content[x] for x in requirement]
    df = pd.DataFrame(res)
    cols = df.columns.tolist()
    cols.insert(0, cols[-1])
    cols.insert(0, cols[-2])
    cols = cols[:-2]
    df = df[cols]
    df.to_csv("./package_requirement.csv")


def write_pk_topic(pk_topic: dict):
    res = {}
    topics = None
    for package in pk_topic:
        res[package] = list(pk_topic[package].values())
        if topics is None:
            topics = list(pk_topic[package].keys())
    res["req_type"] = topics
    df = pd.DataFrame(res)
    cols = df.columns.tolist()
    cols.insert(0, cols[-1])
    cols = cols[:-1]
    df = df[cols]
    df.to_csv("./package_requirement_type.csv")


if __name__ == "__main__":
    data_dir = "../results/vista"
    art_dir = "G:\Download\Vista\Processed"
    dirs = os.listdir(data_dir)
    code_index, packages = build_package_index()  # key is file name ,value is the package name

    pk_req = {}
    pk_topic = {}
    req_index, topics, req_content = readReqTopicIndex()
    for pk in packages:
        pk_req[pk] = {}
        pk_topic[pk] = {}
        for req in req_index.keys():
            pk_req[pk][req] = 0
        for topic in topics:
            pk_topic[pk][topic] = 0
    cnt = 0
    for dir in dirs:
        if "code_req" in dir:
            print(cnt)
            cnt += 1
            batch_dir = os.path.join(data_dir, dir)
            files = os.listdir(batch_dir)
            for file in files:
                if not file.endswith(".csv"):
                    continue
                file_path = os.path.join(batch_dir, file)
                with open(file_path) as fin:
                    for i, line in enumerate(fin):
                        code_id, req_id, score = line.strip("\n\t\r").split(",")
                        if (not code_id.endswith(".zwr") and not code_id.endswith(".m")) or req_id not in req_index:
                            continue
                        package_name = code_index[code_id]
                        topic = req_index[req_id]
                        pk_req[package_name][req_id] += float(score)
                        pk_topic[package_name][topic] += float(score)
    write_pk_req(pk_req, req_content)
    write_pk_topic(pk_topic)
    print("Finish")
