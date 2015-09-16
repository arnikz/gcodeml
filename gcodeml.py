#
# This is a light-weight library to submit/monitor codeml jobs on SMSCG.
#
# Author: Arnold Kuzniar
# Version: 0.1
#

import os
import re
import subprocess, shlex
import time

class xrsl:
   """xRSL class"""
   def __init__(self):
      """Create an instance of the xRSL class.
      """
      self.__executable = 'codeml_worker.pl'
      self.__arguments = []
      self.__inputfiles = []
      self.__outputfiles = []
      self.__stderr = 'codeml.stderr.txt'
      self.__stdout = 'codeml.stdout.txt'
      self.__gmlog = '.arc'
      self.__rerun = 2
      self.__walltime = None
      self.__runtimeenvironment = 'APPS/BIO/CODEML-4.4.3'
      self.__cluster = None
      self.__jobname = None
      #self.__nodeaccess = '"inbound"|"outbound"' - request clusters with in/out IP connectivity

   #
   # Accessor methods: "getters"
   #
   def getName(self):
      return self.__jobname

   def getExec(self):
      return self.__executable

   def getArgs(self):
      return self.__arguments

   def getInputs(self):
      return self.__inputfiles

   def getOutputs(self):
      return self.__outputfiles

   def getStderr(self):
      return self.__stderr

   def getStdout(self):
      return self.__stdout

   def getLog(self):
      return self.__gmlog

   def getWalltime(self):
      return self.__walltime

   def getNretry(self):
      return self.__rerun

   def getRtenv(self):
      return self.__runtimeenvironment

   def getCluster(self):
      return self.__cluster

   def getXrsl(self):
      """Return xRSL job description string.
      """
      __sep1 = "\" \""
      __sep2 = __sep1 + "\")(\""
      _xrsl_str = """&(executable="%s")
(arguments="%s")
(inputfiles=("%s" ""))
(outputfiles=("%s" ""))
(stdout="%s")
(stderr="%s")
(gmlog="%s")
(rerun="%d")
(runtimeenvironment="%s")
(jobname="%s")""" % (
      xrsl.getExec(self),
      __sep1.join(xrsl.getArgs(self)),
      __sep2.join(xrsl.getInputs(self)),
      __sep2.join(xrsl.getOutputs(self)),
      xrsl.getStdout(self),
      xrsl.getStderr(self),
      xrsl.getLog(self),
      xrsl.getNretry(self),
      xrsl.getRtenv(self),
      xrsl.getName(self))

      # the following attributes are optional, and are shown
      # only when these are set to values different than None
      if xrsl.getWalltime(self):
         _xrsl_str += '(walltime="%s")\n' % xrsl.getWalltime(self)

      if xrsl.getCluster(self):
         _xrsl_str += '(cluster="%s")\n' % xrsl.getCluster(self)

      return _xrsl_str

   def __repr__(self):
      return xrsl.getXrsl(self)

   #
   # Accessor methods: "setters"
   #
   def setName(self, jobname):
      self.__jobname = jobname

   def setExec(self, executable):
      self.__executable = executable

   def setArgs(self, args):
       self.__arguments = args
   
   def setInputs(self, infiles):
      self.__inputfiles = infiles

   def setOutputs(self, outfiles):
      self.__outputfiles = outfiles

   def setStderr(self, stderr):
      self.__stderr = stderr

   def setStdout(self, stdout):
      self.__stdout = stdout

   def setLog(self, log):
      self.__gmlog = log

   def setNretry(self, n):
      self.__rerun = n

   def setWalltime(self, str):
      # TODO: Check if both \s+ and \s* work
      _WALLTIME_RE = re.compile('\d+\s+minute|minutes|hour|hours|day|days|week|weeks')
      if not _WALLTIME_RE.search(str):
         raise RuntimeError('Incorrect value for "walltime" xRSL parameter!')
      self.__walltime = str

   def setRtenv(self, rte):
      self.__runtimeenvironment = rte

   def setCluster(self, cluster):
      # TODO: Exception handling, hostname format, lookup table
      self.__cluster = cluster

class job(xrsl):
   __states = ['NEW', 'SUBMITTED', 'RUNNING', 'TERMINATED']

   def __init__(self, jobname, args, inputfiles, outputfiles):
      self.__stateidx = 0
      self.__gridjobid = None      # gsiftp://...
      self.__returncode = None     # "fake" ngsub exitcode
      self.__cluster = None        # cluster on which the job ran
      self.__status = None         # as reported by ngstat [not implemented]
      self.__timesubmitted = None  # as reported by ngstat [not implemented]
      self.__timecompleted = None  # as reported by ngstat [not implemented]
      self.__executionnode = None  # as reported by ngstat [not implemented]
      self.__alninfo = []          # alignment length &
                                   # number of sequences
      xrsl.__init__(self)
      xrsl.setName(self, jobname)
      xrsl.setArgs(self, args)
      xrsl.setInputs(self, inputfiles)
      xrsl.setOutputs(self, outputfiles)

      # parse alignment file(s) and set alingment info
      _aln_files = self.getInfiles('.phy')
      for f in _aln_files:
         _aln_info = job._parseAlignment(f)
         self.__alninfo.append(_aln_info)
   #
   # Accessor methods: "getters"
   #
   def getState(self):
      return job.__states[self.__stateidx]

   def getId(self):
      return self.__gridjobid

   def getReturncode(self):
      return self.__returncode

   def _getCluster(self):
      return self.__cluster

   def getAlninfo(self):
      return self.__alninfo

   def getAlnlen(self):
       return job.getAlninfo(self)[0]['aln_len']

   def getNseq(self):
       return job.getAlninfo(self)[0]['n_seq']

   #def getAlnfile(self):
   #    return job.getAlninfo(self)[0]['path']

   def getInfiles(self, file_sfx):
      return [ f for f in job.getInputs(self) if f.endswith(file_sfx) ]

   def dump(self):
      for attr in self.__dict__.keys():
         print "%s = %s" % (attr, getattr(self, attr))

   @staticmethod
   def _parseAlignment(path):
      _ALN_INFO_RE = re.compile('(?P<n_seq>\d+)\s+(?P<aln_len>\d+)')
      if not os.path.exists(path):
         raise RuntimeError("No alignment file '%s' found." % path)

      f = open(path, 'r')
      for line in f.readlines():
         match = _ALN_INFO_RE.search(line)
         if match:
            n_seq = match.group('n_seq')
            aln_len = match.group('aln_len')
            break
      f.close()
      return {'n_seq' : n_seq, 'aln_len' : aln_len, 'path' : path}

   #
   # Accessor methods: "setters"
   #
   def nextState(self):
      if len(job.__states) - 1  > self.__stateidx:
         self.__stateidx += 1
      else:
         pass

   def setId(self, jobid):
      self.__gridjobid = jobid

   def setReturncode(self, rc):
      self.__returncode = rc

   def _setCluster(self, cls):
      self.__cluster = cls

class session:
   def __init__(self, name):
      self.__name = name
      self.__jobfile = name + '.jobs' # default *.jobs
      self.__starttime = time.time()  # float time
      self.__endtime = None
      self.__sessiondir = name
      self.__debugmode = 0
      #self.__bundlesize = 1 # jobs per call
      #self.__state = None
      self.__joblist = []
      session._createSessiondir(self) # create session directory

   @staticmethod
   def renewProxy():
      rc = os.system('voms-proxy-info')
      if rc != 0: # no proxy found
         print 'Create Grid proxy...'
         os.system('voms-proxy-init -voms life -valid 24:00')

      rc = os.system('voms-proxy-info|grep 0:00:00')
      if rc is 0: # proxy found but no time left
         print 'Renew Grid proxy...'
         os.system('voms-proxy-init -voms life -valid 24:00')

   #
   # Accessor methods: "getters"
   #
   def getName(self):
      return self.__name

   def countJobs(self):
      return len(self.__joblist)
      
   def getJoblist(self):
      __joblist = []
      for job in self.__joblist:
         j = job.getName()
         __joblist.append(j)
      return __joblist

   def getJobs(self):
      return self.__joblist

   def getStime(self):
      return self.__starttime

   def getEtime(self):
       return self.__endtime

   def getJobfile(self):
      return self.__jobfile
   
   def getSessiondir(self):
      return self.__sessiondir

   def getDbgmode(self):
      return self.__debugmode

   def getStarttime(self):
      return time.ctime(self.__starttime)

   def getEndtime(self):
      if self.__endtime:
         return time.ctime(self.__endtime)
      else:
         return None

   def getDuration(self):
      stm = self.__starttime
      etm = self.__endtime

      if stm and etm:
         return round(etm - stm, 1)
      else:
         return None

   #
   # Accessor methods: "setters":
   #
   def _setJobfile(self, fname):
      self.__jobfile = fname

   def addJob(self, *jobs):
      for job in jobs:
         self.__joblist.append(job)

   def setDbgmode(self, mode):
      self.__debugmode = int(mode)

   def setEndtime(self, tm):
      self.__endtime = tm

   def delJob(self, *jobs):
      for job in jobs:
         self.__joblist.remove(job)
  
   def _createSessiondir(self):
       dir = session.getSessiondir(self)
       if not os.path.exists(dir):
           os.mkdir(dir)
       else:
           raise RuntimeError("Session directory '%s' already exists." % dir)
         
   def submit(self):
      # TODO: 
      #       1. Resubmit failed jobs to different WN.
      #       2. Submit jobs from a slot (a directory with symbolink links to files).

      # renew the Grid proxy if expired
      session.renewProxy()
      
      # set "fake" returncode
      rc = 0

      # regexp to mach jobid and cluster name
      _GSIFTP_RE = re.compile('jobid:\s*(?P<jobid>\S+)', re.I)
      _CLUSTER_RE = re.compile('gsiftp://(?P<cluster>[^:]+)', re.I)

      # write jobID file
      jobfile = session.getJobfile(self)
      dbg = session.getDbgmode(self)
      if os.path.exists(jobfile):
         os.unlink(jobfile)

      # change to session directory (slot)
      session_dir = session.getSessiondir(self)
      #os.chdir(session_dir)

      # build ngsub command-line
      str = 'ngsub -o %s -d %d' % (jobfile, dbg)
      for job in session.getJobs(self):
         # create symlinks to input files in the slot
         #aln_file = job.getInfiles()
         # append comment line with jobname to jobfile
         jobname = job.getName()
         f = open(jobfile, 'a')
         f.write('# jobname=%s\n' % jobname)
         f.close()

         # complete ngsub command-line and execute it
         xrsl_str = job.getXrsl()
         xrsl_str = xrsl_str.replace('\n', '')
         ngsub = str + " -e '%s'" % xrsl_str
         args = shlex.split(ngsub) # tokenize the command-line
         line = subprocess.Popen(args, stdout=subprocess.PIPE).communicate()[0]

         # parse codeml STDOUT
         match_jobid = _GSIFTP_RE.search(line)
         if match_jobid:
            job.setId(match_jobid.group('jobid'))
            match_cluster = _CLUSTER_RE.search(job.getId())
            if match_cluster:
               job._setCluster(match_cluster.group('cluster'))
            job.setReturncode(rc) # submission succeeded
            job.nextState() # move to next state 'SUBMITTED'
         else:
            rc += 1
            job.setReturncode(rc) # submission failed; remain in 'NEW' state

   def monitor(self, timeint=10, *jobnames):
      # TODO:
      #       1. Monitoring based on information in the taskdb rather than ngstat.

      _STATUS_RE = re.compile('Status:\s*(?P<jobstat>\S+)', re.I)
      _GSIFTP_RE = re.compile('Job\s*(?P<jobid>gsiftp\S+)', re.I)
      _JOBNAME_RE = re.compile('Job\s*Name:\s*(?P<jobname>\S+)', re.I)
      _EXITCODE_RE = re.compile('Exit\s*Code:\s*(?P<exitcode>\d+)', re.I)

      ngstat = 'ngstat -l -i %s -d %d' % (session.getJobfile(self), session.getDbgmode(self))
      args = shlex.split(ngstat)

      while True:
         all_done = True
         p = subprocess.Popen(args, stdout = subprocess.PIPE)
         stdout = p.communicate()
         jobs = {} # Note: for many jobs this is memory-intensive
         for entry in stdout:
           if entry is None: continue
           jobid = None
           jobname = None
           jobstat = None
           jobrc = 0
           i = 0
           for field in entry.split('\n'):
              field = field.strip()
              if field in(None, ''): continue

              match_st = _STATUS_RE.search(field)
              if match_st: jobstat = match_st.group('jobstat')

              match_id = _GSIFTP_RE.search(field)
              if match_id: jobid = match_id.group('jobid')

              match_nm = _JOBNAME_RE.search(field)
              if match_nm: jobname = match_nm.group('jobname')

              match_rc = _EXITCODE_RE.search(field)
              if match_rc: jobrc = match_rc.group('exitcode')

              if jobname and jobid and jobstat:
                 jobs[jobid] = [jobname, jobstat, jobrc]

         if len(jobs) == 0: all_done = False
         for jobid, aref in jobs.iteritems():
            i += 1
            jobname = aref[0]
            jobstat = aref[1]
            jobrc = aref[2]
            if jobstat not in ('FINISHED', 'FAILED'): all_done = False
            print '%d. %s %s [%s:%d]' % (i, jobname, jobid, jobstat, int(jobrc))
         print
         
         if all_done:
            session.setEndtime(self, time.time())
            break # all jobs done
         else:
            os.system('sleep %d' % timeint) # not all jobs are done so continue
      # time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
      # Parse stdout for the following lines:
      # Submitted: 2011-10-01 21:09:44
      # Completed: 2011-10-01 21:11:40

#   def bundle(self):
#     """ Two approaches to bundle short-running jobs:
#           1. Expand the xRSL tuples: "inputfiles" and "outputfiles"
#           2. Join together 2 or more xRSL jobs using '&' operator. 
#     """
   def __repr__(self):
      str = "This is a session named '%s'.\n" % self.__name
      str = str + 'It has %d jobs.\n' % session.countJobs(self)

      for job in self.__joblist:
         str = str + `job` + '\n\n'
      return str

# create a taskdb, populate session table, register all jobs, process jobs, remove (or tag as DONE) successfully completed jobs
