import sys


def flush(current_key, values):
    if current_key is None:
        return
    if current_key.startswith("DL:"):
        doc_id = current_key[3:]
        if not values:
            return
        dl = values[0]
        print("DOC_LEN\t%s\t%s" % (doc_id, dl))
    elif current_key.startswith("POST:"):
        term = current_key[5:]
        if not values:
            return
        # df = number of documents containing the term (one line per doc from mappers)
        df = len(values)
        print("VOCAB\t%s\t%s" % (term, df))
        by_doc = {}
        for v in values:
            p = v.split("\t", 1)
            if len(p) != 2:
                continue
            doc_id, tf_s = p[0], p[1]
            by_doc[doc_id] = by_doc.get(doc_id, 0) + int(tf_s)
        for doc_id in sorted(by_doc.keys(), key=lambda x: (len(x), x)):
            print("POST\t%s\t%s\t%s" % (term, doc_id, by_doc[doc_id]))


def main():
    current_key = None
    values = []
    for line in sys.stdin:
        line = line.rstrip("\n\r")
        if not line:
            continue
        tab = line.find("\t")
        if tab == -1:
            continue
        k, v = line[:tab], line[tab + 1 :]
        if k != current_key:
            flush(current_key, values)
            current_key = k
            values = []
        values.append(v)
    flush(current_key, values)


if __name__ == "__main__":
    main()
