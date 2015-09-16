#!/usr/bin/perl
#
# This script executes the codeml program (PAML package; Yang, 1997)
# on the worker nodes of the SMSCGrid and implements post-processing
# on the codeml output files.
#
# Authors: Arnold Kuzniar, Sebastian Moretti & Riccardo Murri
# Version: 1.0
#

use strict;
use warnings;

my $bin = 'codeml';                     # CODEML binary
my $pwd = $ENV{PWD};                    # current path
my $RTE = $ENV{'CODEML_LOCATION'};      # CODEML Runtime Environment (RTE)
my $host = GetHostname();               # get the hostname 
my $cpuinfo = GetCPUinfo();             # get the CPU information
my $binmode = 0555;                     # file permissions
my @temp_files = qw/rub rst rst1 4fold.nuc lnf 2NG.t 2NG.dS 2NG.dN/;
my $failed = 0;
my $H0_RESULT_FILE_SFX = '.H0.mlc';
my $H1_RESULT_FILE_SFX = '.H1.mlc';
my $CODEML;

# Check if the run-time environment (RTE) is set
if ($RTE) {
    $CODEML = "$RTE/$bin"; # prepend the fullpath to codeml binary/executable
} else { # for this to work the codeml binary needs to be transfered to a worker node (WN)
    $CODEML = "$pwd/$bin";
    unless (-e $CODEML) {
        print STDERR "Error: Command '$CODEML' not found.\n";
        exit 127;
    }

    unless (-x $CODEML) { # make the codeml binary executable
        chmod $binmode, $CODEML or die "Error: Failed to make '$CODEML' executable.\n";
    }
}

# Run CODEML sequentially for all control files specified on the command line.
# This could be used e.g., for testing the null (H0) and alternative (H1) hypotheses.
die "Usage: $0 [CONTROL FILE 1]...\n" if @ARGV == 0;

foreach my $ctl(@ARGV) {
    my $stime = GetTime();
    my $etime;
    my $outfile;
    my %codeml_infiles;
    my $exit_code = 0;

    # Print some header info into stdout
    print "COMMAND: $CODEML $ctl\n";
    print "START_TIME: $stime\n";
    print "HOST: $host\n"; # equivalent to 'Execution node'
                           # as shown by ngstat -l
    print "CPU: $cpuinfo\n";

    # Delete temporary files left from previous runs
    unlink @temp_files;

    # Parse control file for I/O file names
    open CTL, $ctl or die "ERROR: Cannot open file '$ctl'.\n"; # exit with 2 if no file
    while (<CTL>) {
        if (/(seqfile|treefile)\s*=\s*(\S+)/i) {
            my $name = $1;
            my $path = $2;
            $codeml_infiles{$name} = $path;
	} elsif (/outfile\s*=\s*(\S+)/i) {
            $outfile = $1;
        }
    }
    close CTL;

    # check if input files do exist
    while (my($name, $path) = each %codeml_infiles) {
        my $cwd = $ENV{PWD};
        my $str = (-e $path) ? 'ok' : 'not found';
        printf("%s: %s [%s]\n", uc($name), $path, $str);
        unless ($str eq 'ok') {
            $exit_code = 3; # missing input files
            print STDERR "Error: File '$path' not found (exit code: $exit_code).\n";
        }
    }      
    print "OUTFILE: $outfile\n";
    exit $exit_code if $exit_code; # don't continue if no input file(s)
    
    # Try to run CODEML
    system($CODEML, $ctl);

    if ($? == -1) {
        # failed to execute CODEML so exit with code 127
        # (in bash it means "command not found")
        $exit_code = 127;
        print STDERR "Error: Command '$CODEML' not found (exit code: $exit_code).\n";
    } elsif ($? != 0) {
        $exit_code = 1;
        print STDERR "Error: Failed to run '$CODEML' on file '$ctl' (exit code: $exit_code).\n";
    } elsif (! -e $outfile) {
        $exit_code = 4;
        print STDERR "Error: Output file '$outfile' not found (exit code: $exit_code).\n";
    } elsif (! IsValid($outfile)) {
        $exit_code = 5;
        print STDERR "Error: '$outfile' is not valid: 'Time used' is missing (exit code: $exit_code).\n";
    }

    $etime = GetTime();
    print "EXIT_CODE: $exit_code\n";
    print "END_TIME: $etime\n";
    exit $exit_code if $exit_code;
}

# Get the CPU information of the worker node (WN)
sub GetCPUinfo { # this works on most distros
    my $cpuinfo = `grep "model name" /proc/cpuinfo|cut -f 2 -d :|sort -u`;
    chomp $cpuinfo;
    return $cpuinfo;
}

# Get WN's hostname
sub GetHostname {
    my $host = `hostname`; # N.B.: This is safer as $ENV{HOSTNAME} might not be set.
    chomp $host;
    return $host;
}

# Request time on WN
sub GetTime {
    my $time = `date "+%F %H:%M:%S"`;
    chomp $time;
    return $time;
}

# Post-processing of the output file (*.mlc)
sub IsValid {
    my $file = shift;
    my $retval = 0; # default False

    open MLC, $file or die "Error: Cannot open codeml output file '$file'.\n";
    while(<MLC>) {
       if (/Time\s+used:\s*\S+/i) { # return True if "Time used" present
            $retval = 1;
            last;
       }
    }
    close MLC;
    return $retval;
}

