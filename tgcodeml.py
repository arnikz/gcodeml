#!/usr/bin/env python
#
# Test gcodeml driver script. Adopt it to your needs to make gcodeml CLI.
#
# Author: Arnold Kuzniar
# Year: 2011
# Version: 0.1
#

import gcodeml

def main():
   # set file prefix/suffix names
   data_pfx = 'FAM_1.'
   ctl_sfx = '.ctl'
   mlc_sfx = '.mlc'
   h0_sfx = '.H0'
   h1_sfx = '.H1'
   tree_sfx = '.nwk'
   aln_sfx = '.phy'
   #data_dir  = '' # not supported yet

   s = gcodeml.session('test-session') # create gcodeml session object
   for i in range(1, 4):
      # set gcodeml args, input and outputs
      jobnm = data_pfx + str(i)
      basenm = jobnm
      ctl_h0 = basenm + h0_sfx + ctl_sfx
      ctl_h1 = basenm + h1_sfx + ctl_sfx
      tree = basenm + tree_sfx
      aln = basenm + aln_sfx
      mlc_h0 = basenm + h0_sfx + mlc_sfx
      mlc_h1 = basenm + h1_sfx + mlc_sfx
      args = [ctl_h0, ctl_h1]
      inputs = [ctl_h0, ctl_h1, tree, aln]
      outputs = [mlc_h0, mlc_h1]

      j = gcodeml.job(jobnm, args, inputs, outputs) # create gcodeml job
      #j.setWalltime("2 minutes")        # set walltime limit (optional)
      #j.setCluster("ce.lhep.unibe.ch")  # set target cluster(s) (optional)
      print j
      s.addJob(j) # add job to session
   s.submit()    # submit session
   s.monitor()   # monitor session

   print 'Session started:', s.getStarttime()
   print 'Session ended:', s.getEndtime()
   print 'Session duration:', s.getDuration()
if __name__ == '__main__' : main()
