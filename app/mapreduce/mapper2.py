import sys


def main():
    for line in sys.stdin:
        line = line.rstrip("\n\r")
        if not line.startswith("DOC_LEN\t"):
            continue
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        dl = parts[2].strip()
        if not dl:
            continue
        # Single reduce group
        print("ALL\t%s" % dl)


if __name__ == "__main__":
    main()
