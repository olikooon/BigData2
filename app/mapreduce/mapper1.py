import re
import sys


def tokenize(text):
    if not text:
        return []
    return re.findall(r"[a-z0-9]+", text.lower())


def main():
    for line in sys.stdin:
        line = line.rstrip("\n\r")
        if not line:
            continue
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        doc_id, title, text = parts[0], parts[1], parts[2]
        tokens = tokenize(title) + tokenize(text)
        dl = len(tokens)
        if dl == 0:
            continue
        # Key before first tab: unique per document
        print("DL:%s\t%s" % (doc_id, dl))
        tf = {}
        for t in tokens:
            tf[t] = tf.get(t, 0) + 1
        for term, c in tf.items():
            # Key: POST:<term> — terms are [a-z0-9]+ so no colon inside
            print("POST:%s\t%s\t%s" % (term, doc_id, c))


if __name__ == "__main__":
    main()
