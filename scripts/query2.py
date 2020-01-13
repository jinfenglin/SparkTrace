import math

import pandas as pd

from Preprocessor import Preprocessor
from VSM import VSM


def get_req(query, reqs):
    query = query.lower()
    query_tokens = query.split()
    preprocessor = Preprocessor()
    docs = []
    for x in reqs.values():
        if not isinstance(x, str):
            x = ""
        tmp = " ".join(preprocessor.get_tokens(x))
        docs.append(tmp)
    vsm = VSM("en")
    vsm.build_model(docs)
    scores = []
    for i, id in enumerate(reqs):
        req_tokens = docs[i].split()
        score = vsm._get_doc_similarity(query_tokens, req_tokens)
        scores.append(score)
    df = pd.DataFrame()
    df["req_id"] = reqs.keys()
    df["scores"] = scores
    return df


if __name__ == "__main__":
    query = {
        "commu_query": "Communication messaging messages hl7 protocol synchronous asynchronous tcp transmit send transmission receive interface memory router exchange transport API",
        "archi_query ": "Platform system database client server distributed SQL SOA",
        "security": "access author user inform ensure data authenticate secure system malicious prevent incorrect product ar"
    }
    # links = pd.read_csv("vist_req_code_raw.csv")
    # links = links[links["scores"] > 0]
    # links = links.groupby("req_id")["req_id", "code_id", "scores"].apply(
    #     lambda x: x.nlargest(5, columns=['scores'])).reset_index(
    #     drop=True)
    # links.to_csv("vet_links.csv")
    reqs = pd.read_csv("package_requirement.csv")

    req_dict = {}
    for id, content in zip(reqs["req_id"], reqs["req_content"]):
        req_dict[id] = content
    links = pd.read_csv("vet_links.csv")
    link_dict = dict()
    for code_id, req_id in zip(links["code_id"], links["req_id"]):
        codes = link_dict.get(req_id, set())
        codes.add(code_id)
        link_dict[req_id] = codes

    for q in query:
        res = []
        df = pd.DataFrame()
        req_rel_scores = get_req(query[q], req_dict)
        req_rel_scores = req_rel_scores.sort_values(by=["scores"], ascending=False)
        req_rel_scores.to_csv(q + "_req_relevance.csv")
        for id, score in zip(req_rel_scores["req_id"], req_rel_scores["scores"]):
            if score > 0.1:
                if id in link_dict:
                    code = link_dict[id]
                    res.extend(code)
        df = pd.DataFrame()
        df["query_related_code"] = res
        df.to_csv(q + "_related_code_files.csv")
