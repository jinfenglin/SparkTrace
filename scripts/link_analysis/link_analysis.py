import math
import os
import random
import shutil

import pandas as pd

from Preprocessor import Preprocessor
from VSM import VSM


def build_package_index():
    vista_code = "G:\Download\VistA-M-master\Packages"
    index = {}
    code_path = {}
    packages = os.listdir(vista_code)
    for pk in packages:
        pk_path = os.path.join(vista_code, pk)
        for dirName, subdirList, fileList in os.walk(pk_path):
            for fname in fileList:
                index[fname] = pk
                code_path[fname] = os.path.join(dirName, fname)
    return index, packages, code_path


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
        if not isinstance(content, str):
            content = " "
        req_content[req_id] = content
    return req_index, topics, req_content


def get_high_value_tokens(code_vec, req_vec, vsm):
    """
    Find the terms with high weight and rank them according to the weight
    :param code_vec:
    :param req_vec:
    :return:
    """
    code_dict = dict(code_vec)
    req_dict = dict(req_vec)
    res = []
    for tk in code_dict:
        if tk in req_dict:
            req_score = req_dict[tk]
            code_score = code_dict[tk]
            tk_value = req_score * code_score
            word = vsm.tfidf_model.id2word[tk]
            entry = (word, req_score, code_score, tk_value)
            res.append(entry)
    res = sorted(res, key=lambda x: x[1], reverse=True)
    return res


if __name__ == "__main__":
    code_index, packages, code_path = build_package_index()  # key is file name ,value is the package name
    req_index, topics, req_content = readReqTopicIndex()
    code_list = random.sample(list(code_index.keys()), 10)
    code_list.append("OOPSGUIT.m")
    code_content = {}
    is_reuse = False

    code_df_file_name = "code_df.csv"
    req_df_file_name = "req_df.csv"
    default_df_schema = ["id", "content"]
    if os.path.isfile(code_df_file_name) and os.path.isfile(req_df_file_name) and is_reuse:
        code_df = pd.read_csv(code_df_file_name)
        req_df = pd.read_csv(req_df_file_name)
        code_def = code_df.fillna("")
        req_df = req_df.fillna("")
        code_content = dict((id, content) for id, content in zip(code_df.id, code_df.content))
        req_content = dict((id, content) for id, content in zip(req_df.id, req_df.content))
    else:
        preprocessor = Preprocessor()
        for code_id in code_list:
            with open(code_path[code_id], encoding='utf8', errors="ignore") as fin:
                code_content[code_id] = " ".join(preprocessor.get_stemmed_tokens(fin.read(), "en"))
        code_df = pd.DataFrame(list(code_content.items()), columns=default_df_schema)
        code_df.to_csv(code_df_file_name)

        for req_id in req_content:
            req_content[req_id] = " ".join(preprocessor.get_stemmed_tokens(req_content[req_id], "en"))
        req_df = pd.DataFrame(list(req_content.items()), columns=default_df_schema)
        req_df.to_csv(req_df_file_name)

    vsm = VSM("en")
    docs = []
    docs.extend(list(req_content.values()))
    docs.extend(list(code_content.values()))
    vsm.build_model(docs)

    cnt = 0

    req_vec_dict = dict()
    for code_id in code_content:
        cnt += 1
        output_dir = os.path.join("res", str(cnt))
        if os.path.isdir(output_dir):
            shutil.rmtree(output_dir)
        os.makedirs(output_dir)
        links = []
        code_tokens = code_content[code_id].split()
        code_vec = vsm.tfidf_model[vsm.tfidf_model.id2word.doc2bow(code_tokens)]

        for req_id in req_content:
            req_tokens = req_content[req_id].split()
            score = vsm._get_doc_similarity(code_tokens, req_tokens)
            req_vec = req_vec_dict.get(req_id,
                                       vsm.tfidf_model[vsm.tfidf_model.id2word.doc2bow(req_tokens)])
            high_value_tokens = get_high_value_tokens(code_vec, req_vec, vsm)
            links.append((req_id, score, req_vec, code_vec, high_value_tokens))
        links = sorted(links, key=lambda x: x[1], reverse=True)

        link_df = pd.DataFrame()
        link_df["req_id"] = [x[0] for x in links]
        link_df["score"] = [x[1] for x in links]
        link_df["high_value_tokens"] = [x[4] for x in links]
        link_df.to_csv(os.path.join(output_dir, "{}_links.csv".format(code_id)))

    print("Finish")
