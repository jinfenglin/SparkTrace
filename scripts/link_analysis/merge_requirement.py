"""
Processed-Vista-NEW.xml
requirement file have duplicated id
"""
import re
import pandas as pd

with open("F:\Download\Vista\Processed\Processed-Vista-NEW.xml", encoding="utf8") as fin:
    visited = set()
    cnt = 1
    df = pd.DataFrame()
    ids = []
    content = []
    for line in fin:
        id = re.search("<art_id>([^</]*)</art_id>", line)
        if id is not None:
            id = id.group(1)
            artTitle = re.search("<art_title>([^</]*)</art_title>", line).group(1)
            if id in visited:
                id += "-" + str(cnt)
                cnt += 1
            visited.add(id)
            ids.append(id)
            content.append(artTitle)
            print(id, artTitle)
    df["req_id"] = ids
    df["req_content"] = content
    df.to_csv("F:\\Download\\Vista\\Processed\\vista_requirement.csv", index=False)
