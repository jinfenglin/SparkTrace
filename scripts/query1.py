import pandas as pd

if __name__ == "__main__":
    df = pd.read_csv("package_requirement.csv")
    res = pd.DataFrame()
    coverage_cnt = []
    req_ids = []
    threshold = 5
    for index, row in df.iterrows():
        cnt = 0
        for col_index in range(3, len(df.columns)):
            if row[col_index] > threshold:
                cnt += 1
        req_ids.append(row[1])
        coverage_cnt.append(cnt)

    res["req_id"] = req_ids
    res["coverage_cnt"] = coverage_cnt
    res["coverage_percent"] = [x/140 for x in coverage_cnt]
    res = res.sort_values(by = ["coverage_cnt"], ascending=False)

    res.to_csv("coverage.csv")
