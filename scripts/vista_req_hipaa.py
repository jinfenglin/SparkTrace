import os
import pandas

from vista_req_code import readReqTopicIndex


def write_res(res: dict, req_content_dict):
    df = pandas.DataFrame()

    req_ids = None
    req_content = []
    for hippa_id in res.keys():
        hippa_req_score = res[hippa_id].values()
        if req_ids is None:
            req_ids = res[hippa_id].keys()
        df[hippa_id] = hippa_req_score

    for req_id in req_ids:
        if req_id in req_content_dict:
            req_content.append(req_content_dict[req_id])
        else:
            req_content.append("Unknow")

    df["req_id"] = req_ids
    df["req_content"] = req_content

    cols = df.columns.tolist()
    cols.insert(0, cols[-1])
    cols.insert(0, cols[-2])
    cols = cols[:-2]
    df = df[cols]

    df.to_csv("req_hippa.csv")


if __name__ == "__main__":
    data_dir = "../results/vista"
    dirs = os.listdir(data_dir)
    req_index, topics, req_content = readReqTopicIndex()
    res = {}

    for dir in dirs:
        if "req_HIPPA" in dir:
            batch_dir = os.path.join(data_dir, dir)
            files = os.listdir(batch_dir)
            for file in files:
                if not file.endswith(".csv"):
                    continue
                file_path = os.path.join(batch_dir, file)
                with open(file_path, encoding='utf8') as fin:
                    for i, line in enumerate(fin):
                        if i == 0:
                            continue
                        req_id, hippa_id, score = line.strip("\n\t\r").split(",")
                        req_score_dict = res.get(hippa_id, {})
                        req_score_dict[req_id] = score
                        res[hippa_id] = req_score_dict
    write_res(res, req_content)
