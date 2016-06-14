"""
Interface to parse *.ffacands files, combined and 
sifted candidates produced by FFA_final for survey 
pointings.

Adopted from accelcands.py
Chitrang Patel, June. 10, 2016
"""

import os.path
import sys
import re
import types


dmhit_re = re.compile(r'^ *DM= *(?P<dm>[^ ]*) *SNR= *(?P<snr>[^ ]*) *\** *$')
candinfo_re = re.compile(r'^(?P<ffafile>.*):(?P<candnum>\d*) *(?P<period>[^ ]*)' \
                         r' *(?P<snr>[^ ]*) *(?P<dm>[^ ]*) *(?P<dt>[^ ]*)' \
                         r' *\((?P<numhits>\d*)\)$')


class FFACand(object):
    """Object to represent candidates as they are listed
        in *.ffacands files.
    """
    def __init__(self, ffafile, candnum, period, dm, snr, \
                                         *args, **kwargs):
        self.ffafile = ffafile
        self.candnum = int(candnum)
        self.dm = float(dm)
        self.snr = float(snr)
        self.sigma = "NULL"
        self.numharm = 1
        self.ipow = "NULL"
        self.cpow = "NULL"
        self.period = float(period)
        self.r = "NULL"
        self.z = "NULL"
        self.dmhits = []

    def add_dmhit(self, dm, snr, sigma):
        self.dmhits.append(DMHit(dm, snr))

    def __str__(self):
        cand = self.ffafile + ':' + `self.candnum`
        result = "%-65s   %7.2f  %6.2f  %s  %s   %s  " \
                 "%s  %12.6f  %s  %s (%d)\n" % \
            (cand, self.dm, self.snr, self.sigma, \
                "%2d".center(7) % self.numharm, self.ipow, \
                self.cpow, self.period*1000.0, self.r, self.z, \
                len(self.dmhits))
        for dmhit in self.dmhits:
            result += str(dmhit)
        return result

    def __cmp__(self, other):
        """By default candidates are sorted by increasing SNR.
        """
        return cmp(self.snr, other.snr)


class FFACandlist(list):
    def __init__(self, *args, **kwargs):
        super(FFACandlist, self).__init__(*args, **kwargs)

    def __getattr__(self, key):
        return np.array([getattr(c, key) for c in self])

    def write_candlist(self, fn=sys.stdout):
        """Write FFACandlist to a file with filename fn.
 
            Inputs:
                fn - path of output candlist, or an open file object
                    (Default: standard output stream)
            NOTE: if fn is an already-opened file-object it will not be
                    closed by this function.
        """
        if type(fn) == types.StringType:
            toclose = True
            file = open(fn, 'w')
        else:
            # fn is actually a file-object
            toclose = False
            file = fn
 
        # Print column headers
        file.write("#" + "file:candnum".center(66) + "P(ms)".center(14) + \
                   "SNR".center(8) + "DM".center(9) + "dt(ms)".center(9) + \
                   "numhits".center(9) + "\n")

        self.sort(reverse=True) # Sort cands by decreasing simga
        for cand in self:
            cand.dmhits.sort()
            file.write(str(cand))
        if toclose:
            file.close()


class DMHit(object):
    """Object to represent a DM hit of a ffacands candidate.
    """
    def __init__(self, dm, snr):
        self.dm = float(dm)
        self.snr = float(snr)

    def __str__(self):
        result = "  DM=%6.2f SNR=%5.2f" % (self.dm, self.snr)
        result += "   " + int(self.snr/3.0)*'*' + '\n'
        return result

    def __cmp__(self, other):
        """By default DM hits are sorted by DM.
        """
        return cmp(self.dm, other.dm)


class FFAcandsError(Exception):
    """An error to throw when a line in a *.ffacands file
        has an unrecognized format.
    """
    pass


def parse_candlist(candlistfn):
    """Parse candidate list and return a list of FFACand objects.
        
        Inputs:
            candlistfn - path of candlist, or an open file object
    
        Outputs:
            An FFACandlist object
    """
    if type(candlistfn) == types.StringType or type(candlistfn) == types.UnicodeType:
        candlist = open(candlistfn, 'r')
        toclose = True
    else:
        # candlistfn is actually a file-object
        candlist = candlistfn
        toclose = False
    cands = FFACandlist()
    for line in candlist:
        if not line.partition("#")[0].strip():
            # Ignore lines with no content
            continue
        candinfo_match = candinfo_re.match(line)
        if candinfo_match:
            cdict = candinfo_match.groupdict()
            cdict['period'] = float(cdict['period'])/1000.0 # convert ms to s
            cands.append(FFACand(**cdict))
        else:
            dmhit_match = dmhit_re.match(line)
            if dmhit_match:
                cands[-1].add_dmhit(**dmhit_match.groupdict())
            else:
                raise FFAcandsError("Line has unrecognized format!\n(%s)\n" % line)
    if toclose:
        candlist.close()
    return cands
