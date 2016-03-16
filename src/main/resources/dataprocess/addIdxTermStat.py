year="cw09"
statfile="/GW/D5data-2/khui/cw-docvector-termdf/termstat/term-df-cf-sort-"+year
outfile="/GW/D5data-2/khui/cw-docvector-termdf/termstat/term-df-cf-idx-"+year

outf=open(outfile, 'w')
termidx=0
for line in open(statfile):
    cols = line.split()
    if len(cols) != 3:
        print "Error", line
        continue
    outf.write("\t".join([cols[0], cols[1], cols[2], str(termidx),'\n']))
    termidx += 1
outf.close()

