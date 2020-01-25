import os
import pandas

if __name__ == "__main__":
    # data_dir = "../results/vista"
    # data_dir = "G:\\Projects\\SparkTrace\\results\\vista_q1_final"
    data_dir = "../results/vista_query2/"
    dirs = os.listdir(data_dir)
    cnt = 0
    code_ids = []
    req_ids = []
    scores = []
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
                        if i == 0:
                            continue
                        code_id, req_id, score = line.strip("\n\t\r").split(",")
                        code_ids.append(code_id)
                        req_ids.append(req_id)
                        scores.append(score)
    res = {}
    res["code_id"] = code_ids
    res["req_id"] = req_ids
    res["scores"] = scores
    df = pandas.DataFrame(res)
    df.to_csv("vist_req_code_raw.csv")
