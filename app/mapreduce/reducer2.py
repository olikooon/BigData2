import sys


def main():
    n = 0
    sum_dl = 0
    for line in sys.stdin:
        line = line.rstrip("\n\r")
        if not line:
            continue
        parts = line.split("\t", 1)
        if len(parts) != 2:
            continue
        try:
            dl = int(parts[1])
        except ValueError:
            continue
        n += 1
        sum_dl += dl
    if n > 0:
        dl_avg = sum_dl / float(n)
    else:
        dl_avg = 0.0
    print("GLOBAL\t%d\t%.6f\t%d" % (n, dl_avg, sum_dl))


if __name__ == "__main__":
    main()
