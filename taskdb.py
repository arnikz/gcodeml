#!/usr/bin/env python

"""

The script takes a gcodeml (using GC3Pie) session file as input and stores the information
about jobs in an SQLlite database for downstream analysis.

Author: Arnold Kuzniar
Year: 2011
Version: 0.1

"""

import os
from os.path import join, abspath
import gc3libs
import gc3libs.persistence
from optparse import OptionParser
import sqlite3
import re

def main():
   parser = OptionParser()
   parser.add_option(
      "-s",
      "--session",
      action = 'store',
      dest = "session_path",
      default = os.path.abspath("gcodeml.jobs"), 
      help = "path to gcodeml session")
                       
   parser.add_option(
      "-d",
      "--database",
      action = "store",
      dest = "db_path",
      default = ':memory:',
      help = "path to database file (by default the database is populated in memory)")

   parser.add_option(
      "-v",
      "--verbose",
      action = "count",
      dest = "verbose",
      default = 0,
      help = "print detailed job information onto screen; add/remove 'v' characters to control the verbosity of the output")

   parser.add_option(
      "-r",
      "--read",
      action = "store_true",
      dest = "read_db",
      default = False,
      help = "read an existing database (use the '-d' option) and print the job information onto screen")

   (opt, args) = parser.parse_args()
   sql_create_table = """
   CREATE TABLE IF NOT EXISTS job(
      id                 TEXT [jobID; persistent_id attr (job.xxx)],
      name               TEXT [job name; lrms_jobname attr],
      input_path         TEXT [fullpath to codeml input directory],
      output_path        TEXT [fullpath to codeml output directory],
      state              TEXT [job state as shown by gcodeml],
      mlc_valid_h0       TEXT [valid codeml *.H0.mlc output file; valid attr],
      mlc_valid_h1       TEXT [valid codeml *.H1.mlc output file; valid attr],
      cluster            TEXT [cluster/compute element; resource_name attr],      
      worker             TEXT [hostname of the worker node; hostname attr],      
      cpu                TEXT [CPU model of the worker node; cpuinfo attr],      
      time_submitted     FLOAT [client-side submission (float) time; SUBMITTED timestamp],      
      time_terminated    FLOAT [client-side termination (float) time; TERMINATED timestamp],      
      codeml_walltime_h0 INTEGER [time used by the codeml H0 run (sec); time_used attr],      
      codeml_walltime_h1 INTEGER [time used by the codeml H1 run (sec); time_used attr],
      aln_len            INTEGER [alignment length; aln_info attr],
      n_seq              INTEGER [number of sequences in alignment; aln_info attr]
   );
   """

   sql_create_view_timevar = """
   CREATE VIEW v_jobs_timevar AS
   SELECT
      COUNT(*) n_jobs,
      cluster || ':' || worker || ':' || cpu wn,
      MIN(codeml_walltime_h0+codeml_walltime_h1) codeml_min_time,
      MAX(codeml_walltime_h0+codeml_walltime_h1) codeml_max_time,
      ROUND(AVG(codeml_walltime_h0+codeml_walltime_h1)) codeml_avg_time,
      ROUND(MIN(time_terminated-time_submitted)) gcodeml_min_time,
      ROUND(MAX(time_terminated-time_submitted)) gcodeml_max_time,
      ROUND(AVG(time_terminated-time_submitted)) gcodeml_avg_time
   FROM job GROUP BY wn ORDER BY n_jobs DESC;

   """
   sql_create_view_session = """
   CREATE VIEW v_session AS
   SELECT ROUND(
      MAX(time_terminated)-MIN(time_submitted)) session_walltime,
      DATETIME(MIN(time_submitted), 'unixepoch', 'localtime') session_start_time,
      DATETIME(MAX(time_terminated), 'unixepoch', 'localtime') session_end_time,
      SUM(codeml_walltime_h0+codeml_walltime_h1) cum_codeml_walltime,
      COUNT(DISTINCT(cluster || ':' || worker)) n_workers,
      COUNT(*) n_jobs,
      MIN(codeml_walltime_h0) min_time_h0,
      MAX(codeml_walltime_h0) max_time_h0,
      MIN(codeml_walltime_h1) min_time_h1,
      MAX(codeml_walltime_h1) max_time_h1
   FROM job;
   """ # IF NOT EXISTS clause does not work!

   sql_create_view_failed_jobs = """
   CREATE VIEW v_failed_jobs AS
   SELECT cluster, count(*) n_jobs
   FROM job
   WHERE mlc_valid_h0 IN ('None','False') OR mlc_valid_h1 IN ('None','False')
   GROUP BY cluster ORDER BY n_jobs DESC;
   """

   sql_delete_rows = "DELETE FROM job;"

   sql_insert_row = """
   INSERT INTO job(
      id,
      cluster,
      worker,
      cpu,
      codeml_walltime_h0,
      codeml_walltime_h1,
      time_submitted,
      time_terminated,
      state,
      input_path,
      output_path,
      mlc_valid_h0,
      mlc_valid_h1,
      aln_len,
      n_seq
   ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
   """

   sql_jobs_per_cluster = """
   SELECT
      cluster,
      state,
      COUNT(*) n_jobs
   FROM job GROUP BY cluster, state;
   """

   sql_select_view_session = """
   SELECT
      n_jobs,
      n_workers,
      session_walltime,
      session_start_time,
      session_end_time,
      cum_codeml_walltime,
      min_time_h0,
      max_time_h0,
      min_time_h1,
      max_time_h1
   FROM v_session;
   """

   sql_select_view_timevar = "" # NOT IMPLEMENTED

   def create_taskdb():
      """
      Populate a new 'job' table, 'v_job' view
      and delete rows from previous runs.
      """
      cur = conn.cursor()
      cur.execute(sql_create_table)
      cur.execute(sql_create_view_session)
      cur.execute(sql_create_view_timevar)
      cur.execute(sql_create_view_failed_jobs) 
      cur.execute(sql_delete_rows) 
      mystore=gc3libs.persistence.FilesystemStore(opt.session_path)
      for jobid in mystore.list():
         try:
            job=mystore.load(jobid)
            if job.jobname == "CodemlApplication":
               cur.execute(
                  sql_insert_row,
                  job.persistent_id,
                  getattr(job.execution, 'resource_name', None), # AK: fix "arc_cluster" -> "resource_name" 
                  getattr(job, 'hostname', None),
                  getattr(job, 'cpuinfo', None),
                  getattr(job, 'time_used', 0)[0], # codeml H0 runtime/walltime
                  getattr(job, 'time_used', 0)[1], # codeml H1 runtime/walltime
                  job.execution.timestamp['SUBMITTED'], # gcodeml job start time
                  job.execution.timestamp['TERMINATED'], # gcodeml job end time
                  job.execution.state, # gcodeml job state
                  ",".join(str(url)for url in job.inputs.keys() if url.path.endswith('.ctl')), # input (*.ctl) files separated by comma
                  getattr(job.execution, 'download_dir', None),
                  str(job.valid[0]),
                  str(job.valid[1]),
                  getattr(job, 'aln_info', None)[0]['aln_len'],
                  getattr(job, 'aln_info', None)[0]['n_seq']
               )
         except:
            pass

      conn.commit()

   def nvl(val1, val2):
      """
      Substitue val1 for val2 if val1 is None or 0.
      """
      if val1 is None or val1 == 0:
         return val2
      else:
         return val1

   def print_jobinfo():
      """
      Print jobs summary.
      """
      cur = conn.cursor()
      cur.execute(sql_select_view_session)
      row = cur.fetchone()
      tp = nvl(row['session_walltime'], 'NA')
      ts = nvl(row['cum_codeml_walltime'], 'NA')
      n_workers = nvl(row['n_workers'], 'NA')
      n_jobs = nvl(row['n_jobs'], 'NA')
      tmin_h0 = nvl(row['min_time_h0'], 'NA')
      tmax_h0 = nvl(row['max_time_h0'], 'NA')
      tmin_h1 = nvl(row['min_time_h1'], 'NA')
      tmax_h1 = nvl(row['max_time_h1'], 'NA')
      t_start = nvl(row['session_start_time'], 'NA')
      t_end = nvl(row['session_end_time'], 'NA')

      if tp is not 'NA' and ts is not 'NA' and n_workers is not 'NA':
         speedup = ts / tp
         if speedup >= 1:
            efficiency = "%.2f" % (speedup / n_workers * 100)
            speedup = "%.2f" % speedup
         else:
            efficiency = 'NA' # parallelization unsuccessful
                              # as ts < tp
      else:
        speedup = 'NA'
        efficiency = 'NA'

      print """
# Number of TERMINATED jobs: %s
# Number of distinct workers/cores used: %s
# Session start datetime: %s
# Session end datetime: %s
# Session duration (sec): %s
# Cumulative time of codeml runs (sec): %s [on a single CPU core]
#  H0 runs (sec): min_time=%s\tmax_time=%s
#  H1 runs (sec): min_time=%s\tmax_time=%s
# Speedup factor: %s
# Efficiency: %s
# cluster|job_state|n_jobs
   """ % (n_jobs,
          n_workers,
          t_start,
          t_end,
          tp,
          ts,
          tmin_h0,
          tmax_h0,
          tmin_h1,
          tmax_h1,
          speedup,
          efficiency)
   
      cur.execute(sql_jobs_per_cluster)
      rows = cur.fetchall()
      for r in rows:
         print "%s|%s|%d" % (r[0], r[1], r[2])

### Main ###

   if opt.read_db is True and opt.db_path is ':memory:':
      parser.error("option -r requires also option -d")
   
   if opt.read_db is True and not os.path.isfile(opt.db_path):
      parser.error("database file '%s' does not exist" % opt.db_path)

   if not os.path.exists(opt.session_path) and opt.read_db is False:
      parser.error("session path '%s'does not exist" % opt.session_path)

   conn = sqlite.connect(opt.db_path)
   if opt.read_db is False:
      create_taskdb()
   print_jobinfo()
   conn.close()
if __name__ == '__main__' : main()

